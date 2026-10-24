package com.producer.app.batch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.producer.app.model.PatientModel;
import com.producer.app.repository.FileManagerRepository;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import tools.jackson.databind.json.JsonMapper;

import org.apache.kafka.clients.admin.NewTopic;

@Configuration
@EnableScheduling
public class BatchConfig {

	private static final int PARTITION_THREADS = 2;
	private static final int PARTITION_MAX_THREADS = 5;

	@Bean
	public NewTopic amirDlqTopic(@Value("${batch.dlq-topic}") String topic) {
		return TopicBuilder.name(topic).partitions(1).replicas(1).build();
	}

	@Bean
	public Set<String> seenReferenceIds() {
		return ConcurrentHashMap.newKeySet();
	}

	@Bean
	public TaskExecutor partitionExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(PARTITION_THREADS);
		executor.setMaxPoolSize(PARTITION_MAX_THREADS);
		executor.setQueueCapacity(20);
		executor.setThreadNamePrefix("patient-partition-");
		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		executor.initialize();
		return executor;
	}

	@Bean
	@StepScope
	public Reader partitionReader(@Value("#{stepExecutionContext['fileName']}") String fileName,
			@Value("#{stepExecutionContext['startByte']}") long startByte,
			@Value("#{stepExecutionContext['endByte']}") long endByte,
			@Value("#{stepExecutionContext['firstPartition']}") String firstPartition,
			@Value("#{stepExecutionContext['headerLine']}") String headerLine, S3Client s3Client,
			@Value("${s3.bucket}") String bucket, DlqPublisher dlqPublisher) {
		return new Reader(s3Client, bucket, fileName, startByte, endByte, Boolean.parseBoolean(firstPartition),
				headerLine, dlqPublisher);
	}

	@Bean
	@StepScope
	public Processor partitionProcessor(Set<String> seenReferenceIds, DlqPublisher dlqPublisher,
			@Value("${batch.chunk-size}") int chunkSize) {
		return new Processor(seenReferenceIds, dlqPublisher, chunkSize);
	}

	@Bean
	@StepScope
	public KafkaWriter partitionWriter(KafkaTemplate<String, String> kafkaTemplate, JsonMapper jsonMapper,
			FileManagerRepository fileManagerRepository, Processor partitionProcessor, FilePartitioner filePartitioner,
			DlqPublisher dlqPublisher, @Value("${spring.kafka.template.default-topic}") String topic,
			@Value("#{stepExecutionContext['fileName']}") String fileName) {
		return new KafkaWriter(kafkaTemplate, jsonMapper, fileManagerRepository, partitionProcessor, filePartitioner,
				dlqPublisher, topic, fileName);
	}

	@Bean
	public Step drainDlqStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			DlqDrain dlqDrain, Processor partitionProcessor) {
		return new StepBuilder("drainDlq", jobRepository)
				.tasklet((contribution, chunkContext) -> {
					dlqDrain.drain(partitionProcessor);
					return RepeatStatus.FINISHED;
				}, transactionManager)
				.build();
	}

	@Bean
	public Step registerStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			FilePartitioner filePartitioner) {
		return new StepBuilder("registerFiles", jobRepository)
				.tasklet((contribution, chunkContext) -> {
					filePartitioner.registerPatientFiles();
					return RepeatStatus.FINISHED;
				}, transactionManager)
				.build();
	}

	@Bean
	public Step patientWorker(JobRepository jobRepository, PlatformTransactionManager transactionManager, Reader partitionReader,
			Processor partitionProcessor, KafkaWriter partitionWriter, Listener listener,
			ChunkErrorListener chunkErrorListener, @Value("${batch.chunk-size}") int chunkSize,
			@Value("${batch.chunk-retry-limit}") int chunkRetryLimit) {
		return new StepBuilder("patientWorker", jobRepository)
				.<PatientModel, PatientModel>chunk(chunkSize)
				.transactionManager(transactionManager)
				.reader(partitionReader)
				.processor(partitionProcessor)
				.writer(partitionWriter)
				.faultTolerant()
				.retry(SdkClientException.class, IOException.class, UncheckedIOException.class)
				.retryLimit(chunkRetryLimit)
				.skip(SdkClientException.class, IOException.class, UncheckedIOException.class)
				.skipLimit(100)
				.listener(listener)
				.listener(chunkErrorListener)
				.build();
	}

	@Bean
	public Step patientStep(JobRepository jobRepository, Step patientWorker, FilePartitioner filePartitioner,
			TaskExecutor partitionExecutor) {
		return new StepBuilder("patientStep", jobRepository)
				.partitioner("patientWorker", filePartitioner)
				.step(patientWorker)
				.gridSize(PARTITION_MAX_THREADS)
				.taskExecutor(partitionExecutor)
				.aggregator((result, executions) -> {
					if (executions == null) {
						return;
					}
					boolean anyFailed = false;
					boolean anySucceeded = false;
					for (StepExecution execution : executions) {
						if (execution.getStatus().isUnsuccessful()) {
							anyFailed = true;
						}
						else if (execution.getStatus() == BatchStatus.COMPLETED) {
							anySucceeded = true;
						}
						result.setStatus(BatchStatus.max(result.getStatus(), execution.getStatus()));
						result.setExitStatus(result.getExitStatus().and(execution.getExitStatus()));
						result.setFilterCount(result.getFilterCount() + execution.getFilterCount());
						result.setProcessSkipCount(result.getProcessSkipCount() + execution.getProcessSkipCount());
						result.setCommitCount(result.getCommitCount() + execution.getCommitCount());
						result.setRollbackCount(result.getRollbackCount() + execution.getRollbackCount());
						result.setReadCount(result.getReadCount() + execution.getReadCount());
						result.setReadSkipCount(result.getReadSkipCount() + execution.getReadSkipCount());
						result.setWriteCount(result.getWriteCount() + execution.getWriteCount());
						result.setWriteSkipCount(result.getWriteSkipCount() + execution.getWriteSkipCount());
					}
					if (anyFailed && anySucceeded) {
						result.setStatus(BatchStatus.COMPLETED);
						result.setExitStatus(ExitStatus.COMPLETED);
					}
				})
				.build();
	}

	@Bean
	public Job patientJob(JobRepository jobRepository, Step drainDlqStep, Step registerStep, Step patientStep,
			Listener listener) {
		return new JobBuilder("patientJob", jobRepository)
				.start(drainDlqStep)
				.next(registerStep)
				.next(patientStep)
				.listener(listener)
				.build();
	}
}
