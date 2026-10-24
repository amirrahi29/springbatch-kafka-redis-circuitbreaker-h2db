package com.producer.app.batch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamReader;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.infrastructure.item.file.transform.FieldSet;

import com.producer.app.model.PatientModel;

import io.github.resilience4j.retry.annotation.Retry;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class Reader implements ItemStreamReader<PatientModel> {

	private static final Logger log = LoggerFactory.getLogger(Reader.class);

	private final S3Client s3Client;
	private final String bucket;
	private final String fileName;
	private final long startByte;
	private final long endByte;
	private final boolean firstPartition;
	private final String headerLine;
	private final DlqPublisher dlqPublisher;
	private final DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
	private final byte[] buffer = new byte[8192];

	private InputStream input;
	private int bufferPos;
	private int bufferLen = -1;
	private long consumed;
	private boolean exhausted;

	public Reader(S3Client s3Client, String bucket, String fileName, long startByte, long endByte,
			boolean firstPartition, String headerLine, DlqPublisher dlqPublisher) {
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.fileName = fileName;
		this.startByte = startByte;
		this.endByte = endByte;
		this.firstPartition = firstPartition;
		this.headerLine = headerLine;
		this.dlqPublisher = dlqPublisher;
		this.tokenizer.setStrict(false);
	}

	@Override
	@Retry(name = "s3Read", fallbackMethod = "openFallback")
	public void open(ExecutionContext executionContext) {
		dlqPublisher.bindRead(fileName, startByte);
		input = s3Client.getObject(GetObjectRequest.builder()
				.bucket(bucket)
				.key(fileName)
				.range("bytes=" + startByte + "-")
				.build());
		consumed = 0;
		bufferPos = 0;
		bufferLen = -1;
		exhausted = false;
		try {
			if (startByte > 0) {
				discardPartialLine();
			}
			if (firstPartition) {
				String header = readLine();
				if (header == null) {
					exhausted = true;
					return;
				}
				tokenizer.setNames(headerNames(header));
			}
			else {
				tokenizer.setNames(headerNames(headerLine));
			}
		}
		catch (IOException exception) {
			throw new IllegalStateException("Could not open " + fileName + " at byte " + startByte, exception);
		}
	}

	public void openFallback(ExecutionContext executionContext, Throwable failure) {
		log.error("S3 open failed for {} at byte {} after retries", fileName, startByte, failure);
		dlqPublisher.publishFailure(fileName, -1, startByte, dlqPublisher.s3RetryCount(), failure, 0);
		if (failure instanceof RuntimeException runtime) {
			throw runtime;
		}
		throw new IllegalStateException("Could not open " + fileName + " at byte " + startByte, failure);
	}

	@Override
	public PatientModel read() throws Exception {
		if (exhausted) {
			return null;
		}
		long lineStart = startByte + consumed;
		dlqPublisher.bindRead(fileName, lineStart);
		if (lineStart >= endByte) {
			exhausted = true;
			return null;
		}
		try {
			String line = readLine();
			if (line == null) {
				exhausted = true;
				return null;
			}
			return map(tokenizer.tokenize(line));
		}
		catch (Exception exception) {
			dlqPublisher.publishFailure(fileName, -1, lineStart, dlqPublisher.chunkRetryCount(), exception, 0);
			throw exception;
		}
	}

	@Override
	public void update(ExecutionContext executionContext) {
	}

	@Override
	public void close() {
		if (input == null) {
			return;
		}
		try {
			input.close();
		}
		catch (IOException exception) {
			throw new IllegalStateException("Could not close " + fileName, exception);
		}
		finally {
			input = null;
		}
	}

	private void discardPartialLine() throws IOException {
		int value;
		while ((value = readByte()) >= 0 && value != '\n') {
			// The line that crosses startByte belongs to the previous partition.
		}
	}

	private String readLine() throws IOException {
		int value = readByte();
		if (value < 0) {
			return null;
		}
		ByteArrayOutputStream line = new ByteArrayOutputStream();
		while (value >= 0 && value != '\n') {
			if (value != '\r') {
				line.write(value);
			}
			value = readByte();
		}
		return line.toString(StandardCharsets.UTF_8);
	}

	private int readByte() throws IOException {
		if (bufferLen < 0 || bufferPos >= bufferLen) {
			bufferLen = input.read(buffer);
			bufferPos = 0;
			if (bufferLen < 0) {
				return -1;
			}
		}
		consumed++;
		return buffer[bufferPos++] & 0xff;
	}

	private static PatientModel map(FieldSet fieldSet) {
		PatientModel patient = new PatientModel();
		patient.setReferenceId(fieldSet.readString("ReferenceID"));
		patient.setCorrelationId(fieldSet.readString("CorrelationID"));
		patient.setName(fieldSet.readString("Name"));
		patient.setEmail(fieldSet.readString("Email"));
		patient.setPhone(fieldSet.readString("Phone"));
		patient.setSsn(fieldSet.readString("SSN"));
		patient.setDob(fieldSet.readString("DOB"));
		patient.setGender(fieldSet.readString("Gender"));
		return patient;
	}

	private static String[] headerNames(String line) {
		if (line.startsWith("\uFEFF")) {
			line = line.substring(1);
		}
		String[] names = line.split(",", -1);
		for (int i = 0; i < names.length; i++) {
			names[i] = names[i].trim();
		}
		return names;
	}

}
