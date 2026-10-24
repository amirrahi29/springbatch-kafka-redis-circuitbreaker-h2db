package com.producer.app.batch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import io.github.resilience4j.retry.annotation.Retry;

import com.producer.app.model.FileManager;
import com.producer.app.repository.FileManagerRepository;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@Component
public class Listener implements JobExecutionListener, StepExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(Listener.class);

	private final FileManagerRepository fileManagerRepository;
	private final S3Client s3Client;
	private final Set<String> seenReferenceIds;
	private final FilePartitioner filePartitioner;
	private final String bucket;
	private final int chunkSize;
	private final Listener self;
	private final DlqPublisher dlqPublisher;
	private final Map<String, LocalDateTime> fileStepStarted = new ConcurrentHashMap<>();
	private final Map<String, LocalDateTime> fileStepEnded = new ConcurrentHashMap<>();

	public Listener(FileManagerRepository fileManagerRepository, S3Client s3Client, Set<String> seenReferenceIds,
			FilePartitioner filePartitioner, @Lazy Listener self, DlqPublisher dlqPublisher,
			@Value("${s3.bucket}") String bucket, @Value("${batch.chunk-size}") int chunkSize) {
		this.fileManagerRepository = fileManagerRepository;
		this.s3Client = s3Client;
		this.seenReferenceIds = seenReferenceIds;
		this.filePartitioner = filePartitioner;
		this.self = self;
		this.dlqPublisher = dlqPublisher;
		this.bucket = bucket;
		this.chunkSize = chunkSize;
	}

	@Override
	public void beforeJob(JobExecution jobExecution) {
		seenReferenceIds.clear();
		dlqPublisher.clearPublished();
		filePartitioner.clearPartitionCounts();
		fileStepStarted.clear();
		fileStepEnded.clear();
		log.info("Job started");
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		log.info("Job ended");
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		String fileName = fileName(stepExecution);
		if (fileName == null) {
			return;
		}
		LocalDateTime start = stepExecution.getStartTime() != null ? stepExecution.getStartTime() : LocalDateTime.now();
		fileStepStarted.merge(fileName, start, (left, right) -> left.isBefore(right) ? left : right);
		synchronized (filePartitioner.lock(fileName)) {
			FileManager file = fileManagerRepository.findByFileName(fileName)
					.orElseThrow(() -> new IllegalStateException("filemanager row missing for " + fileName));
			if ("PROCESSING".equals(file.getStatus())) {
				return;
			}
			long totalRecords = self.countDataLines(fileName);
			int totalChunks = totalRecords == 0 ? 0 : (int) ((totalRecords + chunkSize - 1) / chunkSize);
			file.setStatus("PROCESSING");
			file.setProcessed(false);
			file.setTotalRecords(totalRecords);
			file.setTotalChunks(totalChunks);
			file.setPendingChunks(totalChunks);
			file.setProcessedChunks(0);
			file.setFailedChunks(0);
			file.setPassedRecords(0);
			file.setFailedRecords(0);
			file.setDuplicateRecords(0);
			file.setErrorMessage(null);
			file.setCompletedAt(null);
			file.setStartedAt(LocalDateTime.now());
			file.setLastProcessingTimeMs(null);
			file.setRecordsPerSecond(null);
			fileManagerRepository.save(file);
		}
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		String fileName = fileName(stepExecution);
		if (fileName == null) {
			return null;
		}
		synchronized (filePartitioner.lock(fileName)) {
			FileManager file = fileManagerRepository.findByFileName(fileName)
					.orElseThrow(() -> new IllegalStateException("filemanager row missing for " + fileName));
			Long millis = timingMillis(file, fileName, stepExecution);
			Double recordsPerSecond = recordsPerSecond(file, millis);
			if (stepExecution.getStatus() != BatchStatus.COMPLETED) {
				String errorMessage = null;
				if (!stepExecution.getFailureExceptions().isEmpty()
						&& stepExecution.getFailureExceptions().get(0).getMessage() != null) {
					String message = stepExecution.getFailureExceptions().get(0).getMessage();
					errorMessage = message.length() <= 1000 ? message : message.substring(0, 1000);
				}
				fileManagerRepository.markFailed(fileName, errorMessage, millis, recordsPerSecond);
				return null;
			}
			if (filePartitioner.markPartitionFinished(fileName) > 0) {
				fileManagerRepository.updateTiming(fileName, millis, recordsPerSecond);
				return null;
			}
			file = fileManagerRepository.findByFileName(fileName).orElseThrow();
			millis = timingMillis(file, fileName, stepExecution);
			recordsPerSecond = recordsPerSecond(file, millis);
			if (!"FAILED".equals(file.getStatus())) {
				fileManagerRepository.markCompleted(fileName, LocalDateTime.now(), millis, recordsPerSecond);
			}
			else {
				fileManagerRepository.updateTiming(fileName, millis, recordsPerSecond);
			}
		}
		return null;
	}

	private Long timingMillis(FileManager file, String fileName, StepExecution stepExecution) {
		LocalDateTime end = stepExecution.getEndTime() != null ? stepExecution.getEndTime() : LocalDateTime.now();
		LocalDateTime latestEnd = fileStepEnded.merge(fileName, end, (left, right) -> left.isAfter(right) ? left : right);
		LocalDateTime started = fileStepStarted.get(fileName);
		if (started == null) {
			started = stepExecution.getStartTime() != null ? stepExecution.getStartTime() : file.getStartedAt();
		}
		if (started == null) {
			return file.getLastProcessingTimeMs();
		}
		return Math.max(0, Duration.between(started, latestEnd).toMillis());
	}

	private static Double recordsPerSecond(FileManager file, Long millis) {
		if (millis == null) {
			return file.getRecordsPerSecond();
		}
		if (millis == 0) {
			return null;
		}
		long records = file.getPassedRecords() + file.getFailedRecords() + file.getDuplicateRecords();
		return records * 1000.0 / millis;
	}

	private static String fileName(StepExecution stepExecution) {
		ExecutionContext context = stepExecution.getExecutionContext();
		if (!context.containsKey("fileName")) {
			return null;
		}
		return context.getString("fileName");
	}

	@Retry(name = "s3Read", fallbackMethod = "countDataLinesFallback")
	public long countDataLines(String fileName) {
		GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(fileName).build();
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(s3Client.getObject(request), StandardCharsets.UTF_8))) {
			if (reader.readLine() == null) {
				return 0;
			}
			long total = 0;
			while (reader.readLine() != null) {
				total++;
			}
			return total;
		}
		catch (IOException exception) {
			throw new UncheckedIOException(exception);
		}
	}

	public long countDataLinesFallback(String fileName, Throwable failure) {
		log.error("S3 row count failed for {} after retries", fileName, failure);
		dlqPublisher.publishFailure(fileName, -1, -1L, dlqPublisher.s3RetryCount(), failure, 0);
		if (failure instanceof RuntimeException runtime) {
			throw runtime;
		}
		throw new IllegalStateException("Could not count rows in " + fileName, failure);
	}

}
