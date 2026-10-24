package com.producer.app.batch;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.producer.app.model.PatientModel;
import com.producer.app.repository.FileManagerRepository;

import tools.jackson.databind.json.JsonMapper;

@Component
public class DlqDrain {

	private static final Logger log = LoggerFactory.getLogger(DlqDrain.class);

	private final DlqPublisher dlqPublisher;
	private final KafkaTemplate<String, String> kafkaTemplate;
	private final JsonMapper jsonMapper;
	private final FileManagerRepository fileManagerRepository;
	private final String bootstrapServers;
	private final String dlqTopic;
	private final String mainTopic;

	public DlqDrain(DlqPublisher dlqPublisher, KafkaTemplate<String, String> kafkaTemplate, JsonMapper jsonMapper,
			FileManagerRepository fileManagerRepository,
			@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
			@Value("${batch.dlq-topic}") String dlqTopic,
			@Value("${spring.kafka.template.default-topic}") String mainTopic) {
		this.dlqPublisher = dlqPublisher;
		this.kafkaTemplate = kafkaTemplate;
		this.jsonMapper = jsonMapper;
		this.fileManagerRepository = fileManagerRepository;
		this.bootstrapServers = bootstrapServers;
		this.dlqTopic = dlqTopic;
		this.mainTopic = mainTopic;
	}

	public void drain(Processor processor) {
		dlqPublisher.beginDrain();
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps())) {
			consumer.subscribe(List.of(dlqTopic));
			int emptyPolls = 0;
			while (emptyPolls < 3) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
				if (records.isEmpty()) {
					emptyPolls++;
					continue;
				}
				emptyPolls = 0;
				for (ConsumerRecord<String, String> record : records) {
					if (!replay(processor, record)) {
						return;
					}
					consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()),
							new OffsetAndMetadata(record.offset() + 1)));
				}
			}
		}
		catch (Exception exception) {
			dlqPublisher.skipped("unknown", null, -1, -1L, 0, 0, exception);
			log.warn("DLQ drain skipped, main job will continue", exception);
		}
		finally {
			dlqPublisher.endDrain();
		}
	}

	private boolean replay(Processor processor, ConsumerRecord<String, String> record) {
		DlqMessage message;
		try {
			message = jsonMapper.readValue(record.value(), DlqMessage.class);
		}
		catch (RuntimeException exception) {
			dlqPublisher.skipped("unknown", null, -1, record.offset(), 0, 1, exception);
			return false;
		}
		if (message.getRecord() == null) {
			return true;
		}
		try {
			long failedBefore = processor.getFailedRecords();
			long duplicatesBefore = processor.getDuplicateRecords();
			PatientModel processed = processor.process(message.getRecord());
			long failedDelta = processor.getFailedRecords() - failedBefore;
			long duplicateDelta = processor.getDuplicateRecords() - duplicatesBefore;
			long passed = 0;
			if (processed != null) {
				publishToMainTopic(message, processed);
				passed = 1;
			}
			if (message.getFileName() != null) {
				fileManagerRepository.incrementRecovered(message.getFileName(), passed, failedDelta, duplicateDelta);
			}
			return true;
		}
		catch (Exception exception) {
			dlqPublisher.skipped(message.getFileName(), message.getReferenceId(), message.getChunkId(),
					message.getByteOffset(), message.getRetryCount(), message.getDlqRows(), exception);
			return false;
		}
	}

	private void publishToMainTopic(DlqMessage message, PatientModel processed) throws Exception {
		Map<String, Object> payload = new LinkedHashMap<>();
		payload.put("fileName", message.getFileName());
		payload.put("chunkId", message.getChunkId());
		payload.put("recordCount", 1);
		payload.put("records", List.of(processed));
		String json = jsonMapper.writeValueAsString(payload);
		kafkaTemplate.send(mainTopic, message.getFileName(), json).get(2, TimeUnit.MINUTES);
		log.info("Published chunk file={} chunkId={} records={} payload={}", message.getFileName(),
				message.getChunkId(), 1, json);
	}

	private Map<String, Object> consumerProps() {
		Map<String, Object> props = new LinkedHashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, dlqTopic + "-drain");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
		return props;
	}

}
