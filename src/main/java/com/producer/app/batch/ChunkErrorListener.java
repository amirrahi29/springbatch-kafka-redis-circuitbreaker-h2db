package com.producer.app.batch;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.ChunkListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.stereotype.Component;

import com.producer.app.model.PatientModel;

@Component
public class ChunkErrorListener implements ChunkListener<PatientModel, PatientModel>, StepExecutionListener {

	private final DlqPublisher dlqPublisher;
	private final ThreadLocal<StepExecution> stepExecution = new ThreadLocal<>();

	public ChunkErrorListener(DlqPublisher dlqPublisher) {
		this.dlqPublisher = dlqPublisher;
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		this.stepExecution.set(stepExecution);
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		this.stepExecution.remove();
		return stepExecution.getExitStatus();
	}

	@Override
	public void onChunkError(Exception exception, Chunk<PatientModel> chunk) {
		StepExecution execution = stepExecution.get();
		String fileName = fileName(execution);
		if (fileName == null) {
			fileName = dlqPublisher.boundFile();
		}
		int chunkId = execution == null ? -1 : (int) execution.getCommitCount() + 1;
		long byteOffset = dlqPublisher.boundOffset();
		int rows = chunk.size();
		for (PatientModel item : chunk.getItems()) {
			if (item == null) {
				continue;
			}
			dlqPublisher.publishRow(fileName, item, chunkId, byteOffset, dlqPublisher.chunkRetryCount(), exception,
					rows);
		}
	}

	private static String fileName(StepExecution stepExecution) {
		if (stepExecution == null) {
			return null;
		}
		ExecutionContext context = stepExecution.getExecutionContext();
		if (!context.containsKey("fileName")) {
			return null;
		}
		return context.getString("fileName");
	}

}
