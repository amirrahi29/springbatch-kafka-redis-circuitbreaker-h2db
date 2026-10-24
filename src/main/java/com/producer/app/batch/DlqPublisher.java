package com.producer.app.batch;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.producer.app.model.PatientModel;

import tools.jackson.databind.json.JsonMapper;

@Component
public class DlqPublisher {

	public static final String TOPIC = "amir-dlq-topic";

	private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);
	private static final ZoneId ALERT_ZONE = ZoneId.of("Asia/Kolkata");

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final JsonMapper jsonMapper;
	private final String topic;
	private final int s3RetryCount;
	private final int kafkaRetryCount;
	private final int chunkRetryCount;
	private final ThreadLocal<Boolean> draining = ThreadLocal.withInitial(() -> false);
	private final ThreadLocal<String> boundFile = new ThreadLocal<>();
	private final ThreadLocal<Long> boundOffset = ThreadLocal.withInitial(() -> -1L);
	private final Set<String> published = ConcurrentHashMap.newKeySet();

	public DlqPublisher(KafkaTemplate<String, String> kafkaTemplate, JsonMapper jsonMapper,
			@Value("${batch.dlq-topic}") String topic,
			@Value("${resilience4j.retry.instances.s3Read.maxAttempts}") int s3RetryCount,
			@Value("${spring.kafka.producer.retries}") int kafkaRetryCount,
			@Value("${batch.chunk-retry-limit}") int chunkRetryCount) {
		this.kafkaTemplate = kafkaTemplate;
		this.jsonMapper = jsonMapper;
		this.topic = topic;
		this.s3RetryCount = s3RetryCount;
		this.kafkaRetryCount = kafkaRetryCount;
		this.chunkRetryCount = chunkRetryCount;
	}

	public int s3RetryCount() {
		return s3RetryCount;
	}

	public int kafkaRetryCount() {
		return kafkaRetryCount;
	}

	public int chunkRetryCount() {
		return chunkRetryCount;
	}

	public void beginDrain() {
		draining.set(true);
	}

	public void endDrain() {
		draining.set(false);
	}

	public boolean isDraining() {
		return draining.get();
	}

	public void bindRead(String fileName, long byteOffset) {
		boundFile.set(fileName);
		boundOffset.set(byteOffset);
	}

	public String boundFile() {
		return boundFile.get();
	}

	public long boundOffset() {
		return boundOffset.get();
	}

	public void clearPublished() {
		published.clear();
	}

	public void publishRow(String fileName, PatientModel row, int chunkId, long byteOffset, int retryCount,
			Throwable failure, int dlqRows) {
		if (isDraining()) {
			return;
		}
		String referenceId = row == null ? null : row.getReferenceId();
		publish(fileName, referenceId, row, chunkId, byteOffset, retryCount, failure, dlqRows);
	}

	public void publishFailure(String fileName, int chunkId, long byteOffset, int retryCount, Throwable failure,
			int dlqRows) {
		if (isDraining()) {
			return;
		}
		publish(fileName, null, null, chunkId, byteOffset, retryCount, failure, dlqRows);
	}

	public void skipped(String fileName, String referenceId, int chunkId, long byteOffset, int retryCount,
			int dlqRows, Throwable failure) {
		logEvent("skipped", fileName, dlqRows, referenceId, chunkId, OffsetDateTime.now(ALERT_ZONE).toString(),
				byteOffset, retryCount, reason(failure));
	}

	private void publish(String fileName, String referenceId, PatientModel row, int chunkId, long byteOffset,
			int retryCount, Throwable failure, int dlqRows) {
		String failedAt = OffsetDateTime.now(ALERT_ZONE).toString();
		String reason = reason(failure);
		String dedupKey = (fileName == null ? "" : fileName) + "|" + (referenceId == null ? "" : referenceId) + "|"
				+ chunkId + "|" + byteOffset + "|" + reason;
		if (!published.add(dedupKey)) {
			return;
		}
		DlqMessage message = new DlqMessage();
		message.setFileName(fileName);
		message.setReferenceId(referenceId);
		message.setChunkId(chunkId);
		message.setFailedAt(failedAt);
		message.setByteOffset(byteOffset);
		message.setRetryCount(retryCount);
		message.setReason(reason);
		message.setDlqRows(dlqRows);
		message.setRecord(row);
		logEvent("created", fileName, dlqRows, referenceId, chunkId, failedAt, byteOffset, retryCount, reason);
		if (row == null) {
			return;
		}
		try {
			String json = jsonMapper.writeValueAsString(message);
			String key = referenceId == null || referenceId.isBlank() ? fileName : referenceId;
			kafkaTemplate.send(topic, key, json).whenComplete((result, error) -> {
				if (error != null) {
					logEvent("skipped", fileName, dlqRows, referenceId, chunkId, failedAt, byteOffset, retryCount,
							"DLQ publish failed: " + reason(error));
				}
			});
		}
		catch (RuntimeException exception) {
			logEvent("skipped", fileName, dlqRows, referenceId, chunkId, failedAt, byteOffset, retryCount,
					"DLQ publish failed: " + reason(exception));
		}
	}

	private void logEvent(String type, String fileName, int dlqRows, String referenceId, int chunkId, String failedAt,
			long byteOffset, int retryCount, String reason) {
		String reference = (referenceId == null || referenceId.isBlank() ? "UNKNOWN" : referenceId) + "-DLQ";
		log.warn(
				"DLQ event type={} topic={} fileName={} dlqRows={} referenceId={} chunkId={} failedAt={} byteOffset={} retryCount={} reason=\"{}\"",
				type, topic, fileName == null || fileName.isBlank() ? "unknown" : fileName, dlqRows, reference,
				chunkId, failedAt, byteOffset, retryCount, sanitize(reason));
	}

	static String reason(Throwable failure) {
		if (failure == null) {
			return "unknown";
		}
		Throwable root = failure;
		while (root.getCause() != null && root.getCause() != root) {
			root = root.getCause();
		}
		String top = failure.getClass().getSimpleName() + ": " + text(failure.getMessage());
		if (root == failure) {
			return top;
		}
		return top + " | " + root.getClass().getSimpleName() + ": " + text(root.getMessage());
	}

	private static String text(String message) {
		return message == null || message.isBlank() ? "no message" : message;
	}

	private static String sanitize(String reason) {
		return reason.replace('\\', ' ').replace('"', '\'').replace('\n', ' ').replace('\r', ' ');
	}

}
