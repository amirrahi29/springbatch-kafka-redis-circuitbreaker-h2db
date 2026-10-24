package com.producer.app.batch;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.kafka.core.KafkaTemplate;

import com.producer.app.model.PatientModel;
import com.producer.app.repository.FileManagerRepository;

import tools.jackson.databind.json.JsonMapper;

public class KafkaWriter implements ItemWriter<PatientModel> {

	private static final Logger log = LoggerFactory.getLogger(KafkaWriter.class);

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final JsonMapper jsonMapper;
	private final FileManagerRepository fileManagerRepository;
	private final Processor processor;
	private final FilePartitioner filePartitioner;
	private final DlqPublisher dlqPublisher;
	private final String topic;
	private final String fileName;
	private final AtomicInteger chunkId = new AtomicInteger();
	private long savedFailedRecords;
	private long savedDuplicateRecords;

	public KafkaWriter(KafkaTemplate<String, String> kafkaTemplate, JsonMapper jsonMapper,
			FileManagerRepository fileManagerRepository, Processor processor, FilePartitioner filePartitioner,
			DlqPublisher dlqPublisher, String topic, String fileName) {
		this.kafkaTemplate = kafkaTemplate;
		this.jsonMapper = jsonMapper;
		this.fileManagerRepository = fileManagerRepository;
		this.processor = processor;
		this.filePartitioner = filePartitioner;
		this.dlqPublisher = dlqPublisher;
		this.topic = topic;
		this.fileName = fileName;
	}

	@Override
	public void write(Chunk<? extends PatientModel> chunk) throws Exception {
		int id = chunkId.incrementAndGet();
		boolean sent = true;
		if (!chunk.isEmpty()) {
			Map<String, Object> payload = new LinkedHashMap<>();
			payload.put("fileName", fileName);
			payload.put("chunkId", id);
			payload.put("recordCount", chunk.size());
			payload.put("records", chunk.getItems());
			String json = jsonMapper.writeValueAsString(payload);
			try {
				kafkaTemplate.send(topic, fileName, json).get(2, TimeUnit.MINUTES);
				log.info("Published chunk file={} chunkId={} records={} payload={}", fileName, id, chunk.size(), json);
			}
			catch (Exception exception) {
				sent = false;
				if (exception instanceof InterruptedException) {
					Thread.currentThread().interrupt();
				}
				for (PatientModel item : chunk.getItems()) {
					dlqPublisher.publishRow(fileName, item, id, dlqPublisher.boundOffset(), dlqPublisher.kafkaRetryCount(),
							exception, chunk.size());
				}
			}
		}

		long failedNow = processor.getFailedRecords();
		long duplicatesNow = processor.getDuplicateRecords();
		long failedDelta = failedNow - savedFailedRecords;
		long duplicateDelta = duplicatesNow - savedDuplicateRecords;
		savedFailedRecords = failedNow;
		savedDuplicateRecords = duplicatesNow;
		long passed = sent ? chunk.size() : 0;
		long failed = failedDelta + (sent ? 0 : chunk.size());
		synchronized (filePartitioner.lock(fileName)) {
			int updated = fileManagerRepository.incrementCounts(fileName, passed, failed, duplicateDelta);
			if (updated == 0) {
				throw new IllegalStateException("filemanager row missing for " + fileName);
			}
		}
	}

}
