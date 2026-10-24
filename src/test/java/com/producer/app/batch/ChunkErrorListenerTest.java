package com.producer.app.batch;

import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import com.producer.app.model.PatientModel;

@ExtendWith(MockitoExtension.class)
class ChunkErrorListenerTest {

	@Mock
	private DlqPublisher dlqPublisher;

	@Test
	void publishesEveryItemWhenTheChunkRollsBack() {
		ChunkErrorListener listener = new ChunkErrorListener(dlqPublisher);
		ExecutionContext context = new ExecutionContext();
		context.putString("fileName", "patients.csv");
		StepExecution stepExecution = new StepExecution(1L, "patientWorker", null);
		stepExecution.setExecutionContext(context);
		listener.beforeStep(stepExecution);

		PatientModel first = patient("r1");
		PatientModel second = patient("r2");
		RuntimeException failure = new RuntimeException("chunk rolled back");

		listener.onChunkError(failure, new Chunk<>(first, second));

		verify(dlqPublisher).publishRow("patients.csv", first, 1, 0L, 0, failure, 2);
		verify(dlqPublisher).publishRow("patients.csv", second, 1, 0L, 0, failure, 2);
	}

	private static PatientModel patient(String referenceId) {
		PatientModel patient = new PatientModel();
		patient.setReferenceId(referenceId);
		return patient;
	}

}
