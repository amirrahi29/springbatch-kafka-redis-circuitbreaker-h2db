package com.producer.app.batch;

import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.ItemProcessor;

import com.producer.app.model.PatientModel;

public class Processor implements ItemProcessor<PatientModel, PatientModel> {

	private final Set<String> seenReferenceIds;
	private final DlqPublisher dlqPublisher;
	private final int chunkSize;
	private long failedRecords;
	private long duplicateRecords;
	private int calls;

	public Processor(Set<String> seenReferenceIds, DlqPublisher dlqPublisher, int chunkSize) {
		this.seenReferenceIds = seenReferenceIds;
		this.dlqPublisher = dlqPublisher;
		this.chunkSize = chunkSize;
	}

	@Override
	public @Nullable PatientModel process(PatientModel item) {
		calls++;
		int chunkId = ((calls - 1) / chunkSize) + 1;
		try {
			if (!complete(item)) {
				failedRecords++;
				return null;
			}
			if (!seenReferenceIds.add(item.getReferenceId())) {
				duplicateRecords++;
				return null;
			}
			return item;
		}
		catch (RuntimeException exception) {
			if (dlqPublisher.isDraining()) {
				throw exception;
			}
			failedRecords++;
			dlqPublisher.publishRow(dlqPublisher.boundFile(), item, chunkId, dlqPublisher.boundOffset(), 1, exception,
					1);
			return null;
		}
	}

	public long getFailedRecords() {
		return failedRecords;
	}

	public long getDuplicateRecords() {
		return duplicateRecords;
	}

	private static boolean complete(PatientModel item) {
		return present(item.getReferenceId()) && present(item.getCorrelationId()) && present(item.getName())
				&& present(item.getEmail()) && present(item.getPhone()) && present(item.getSsn())
				&& present(item.getDob()) && present(item.getGender());
	}

	private static boolean present(String value) {
		return value != null && !value.isBlank();
	}

}
