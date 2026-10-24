package com.producer.app.batch;

import com.producer.app.model.PatientModel;

public class DlqMessage {

	private String fileName;
	private String referenceId;
	private int chunkId;
	private String failedAt;
	private long byteOffset;
	private int retryCount;
	private String reason;
	private int dlqRows;
	private PatientModel record;

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getReferenceId() {
		return referenceId;
	}

	public void setReferenceId(String referenceId) {
		this.referenceId = referenceId;
	}

	public int getChunkId() {
		return chunkId;
	}

	public void setChunkId(int chunkId) {
		this.chunkId = chunkId;
	}

	public String getFailedAt() {
		return failedAt;
	}

	public void setFailedAt(String failedAt) {
		this.failedAt = failedAt;
	}

	public long getByteOffset() {
		return byteOffset;
	}

	public void setByteOffset(long byteOffset) {
		this.byteOffset = byteOffset;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public int getDlqRows() {
		return dlqRows;
	}

	public void setDlqRows(int dlqRows) {
		this.dlqRows = dlqRows;
	}

	public PatientModel getRecord() {
		return record;
	}

	public void setRecord(PatientModel record) {
		this.record = record;
	}

}
