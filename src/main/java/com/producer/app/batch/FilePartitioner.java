package com.producer.app.batch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import io.github.resilience4j.retry.annotation.Retry;

import com.producer.app.model.FileManager;
import com.producer.app.repository.FileManagerRepository;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

@Component
public class FilePartitioner implements Partitioner {

	static final long SMALL_FILE_BYTES = 1024L * 1024L;
	static final int MAX_PARTITIONS_PER_FILE = 5;

	private static final Logger log = LoggerFactory.getLogger(FilePartitioner.class);

	private final S3Client s3Client;
	private final FileManagerRepository fileManagerRepository;
	private final String bucket;
	private final FilePartitioner self;
	private final DlqPublisher dlqPublisher;
	private final Map<String, AtomicInteger> remainingPartitions = new ConcurrentHashMap<>();
	private final Map<String, Object> fileLocks = new ConcurrentHashMap<>();

	public FilePartitioner(S3Client s3Client, FileManagerRepository fileManagerRepository,
			@Value("${s3.bucket}") String bucket, @Lazy FilePartitioner self, DlqPublisher dlqPublisher) {
		this.s3Client = s3Client;
		this.fileManagerRepository = fileManagerRepository;
		this.bucket = bucket;
		this.self = self;
		this.dlqPublisher = dlqPublisher;
	}

	public void registerPatientFiles() {
		for (S3Object object : self.patientFiles()) {
			String fileName = object.key();
			FileManager file = fileManagerRepository.findByFileName(fileName).orElseGet(FileManager::new);
			if ("COMPLETED".equals(file.getStatus())) {
				continue;
			}
			if (file.getId() == null) {
				file.setFileName(fileName);
				file.setFileType(extension(fileName));
				file.setFileSize(object.size());
				file.setStatus("READY");
				file.setProcessed(false);
				file.setTotalRecords(0);
				file.setPassedRecords(0);
				file.setFailedRecords(0);
				file.setDuplicateRecords(0);
				file.setTotalChunks(0);
				file.setProcessedChunks(0);
				file.setFailedChunks(0);
				file.setPendingChunks(0);
				fileManagerRepository.save(file);
			}
			else if (!"READY".equals(file.getStatus()) && file.getPendingChunks() <= 0) {
				file.setStatus("READY");
				fileManagerRepository.save(file);
			}
		}
	}

	public void clearPartitionCounts() {
		remainingPartitions.clear();
	}

	public Object lock(String fileName) {
		return fileLocks.computeIfAbsent(fileName, key -> new Object());
	}

	public int markPartitionFinished(String fileName) {
		AtomicInteger remaining = remainingPartitions.get(fileName);
		if (remaining == null) {
			return 0;
		}
		return remaining.decrementAndGet();
	}

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> partitions = new LinkedHashMap<>();
		int index = 0;
		for (FileManager file : fileManagerRepository.findAssignable()) {
			long fileSize = file.getFileSize() == null ? 0L : file.getFileSize();
			int parts = partitionCount(fileSize);
			long size = fileSize <= 0 ? 0L : fileSize;
			String headerLine;
			try {
				headerLine = self.readHeader(file.getFileName(), size);
			}
			catch (RuntimeException exception) {
				log.error("Skipping {} after S3 header retries failed", file.getFileName(), exception);
				continue;
			}
			remainingPartitions.put(file.getFileName(), new AtomicInteger(parts));
			for (int part = 0; part < parts; part++) {
				long startByte = size == 0 ? 0L : size * part / parts;
				long endByte = size == 0 ? Long.MAX_VALUE : size * (part + 1) / parts;
				ExecutionContext context = new ExecutionContext();
				context.putString("fileName", file.getFileName());
				context.putLong("startByte", startByte);
				context.putLong("endByte", endByte);
				context.putString("firstPartition", part == 0 ? "true" : "false");
				context.putString("headerLine", headerLine);
				partitions.put("file-" + index, context);
				index++;
			}
		}
		return partitions;
	}

	private static int partitionCount(long fileSize) {
		if (fileSize <= SMALL_FILE_BYTES) {
			return 1;
		}
		long count = (fileSize + SMALL_FILE_BYTES - 1) / SMALL_FILE_BYTES;
		return (int) Math.min(MAX_PARTITIONS_PER_FILE, count);
	}

	@Retry(name = "s3Read", fallbackMethod = "readHeaderFallback")
	public String readHeader(String fileName, long fileSize) {
		long lastByte = fileSize > 0 ? Math.min(fileSize - 1, 16383) : 16383;
		GetObjectRequest request = GetObjectRequest.builder()
				.bucket(bucket)
				.key(fileName)
				.range("bytes=0-" + lastByte)
				.build();
		try (InputStream input = s3Client.getObject(request)) {
			ByteArrayOutputStream header = new ByteArrayOutputStream();
			int value;
			while ((value = input.read()) >= 0 && value != '\n') {
				if (value != '\r') {
					header.write(value);
				}
			}
			return header.toString(StandardCharsets.UTF_8);
		}
		catch (IOException exception) {
			throw new UncheckedIOException(exception);
		}
	}

	public String readHeaderFallback(String fileName, long fileSize, Throwable failure) {
		log.error("S3 header read failed for {} after retries", fileName, failure);
		dlqPublisher.publishFailure(fileName, -1, 0L, dlqPublisher.s3RetryCount(), failure, 0);
		if (failure instanceof RuntimeException runtime) {
			throw runtime;
		}
		throw new IllegalStateException("Could not read header for " + fileName, failure);
	}

	@Retry(name = "s3Read", fallbackMethod = "patientFilesFallback")
	public List<S3Object> patientFiles() {
		List<S3Object> files = new ArrayList<>();
		String token = null;
		do {
			ListObjectsV2Request.Builder request = ListObjectsV2Request.builder().bucket(bucket);
			if (token != null) {
				request.continuationToken(token);
			}
			ListObjectsV2Response response = s3Client.listObjectsV2(request.build());
			for (S3Object object : response.contents()) {
				String key = object.key();
				String lower = key.toLowerCase(Locale.ROOT);
				if (lower.startsWith("patient") && lower.endsWith(".csv")) {
					files.add(object);
				}
			}
			token = response.isTruncated() ? response.nextContinuationToken() : null;
		}
		while (token != null);
		files.sort((left, right) -> left.key().compareTo(right.key()));
		return files;
	}

	public List<S3Object> patientFilesFallback(Throwable failure) {
		log.error("S3 patient file list failed after retries", failure);
		dlqPublisher.publishFailure("unknown", -1, -1L, dlqPublisher.s3RetryCount(), failure, 0);
		if (failure instanceof RuntimeException runtime) {
			throw runtime;
		}
		throw new IllegalStateException("Could not list patient files from S3", failure);
	}

	private static String extension(String fileName) {
		int dot = fileName.lastIndexOf('.');
		if (dot < 0 || dot == fileName.length() - 1) {
			return null;
		}
		return fileName.substring(dot + 1).toLowerCase(Locale.ROOT);
	}

}
