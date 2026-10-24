package com.producer.app.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "filemanager")
public class FileManager {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false, unique = true)
	private String fileName;

	private String fileType;

	private Long fileSize;

	@Column(nullable = false)
	private String status;

	private boolean processed;

	private long totalRecords;

	private long passedRecords;

	private long failedRecords;

	private long duplicateRecords;

	private int totalChunks;

	private int processedChunks;

	private int failedChunks;

	private int pendingChunks;

	@Column(length = 1000)
	private String errorMessage;

	private LocalDateTime startedAt;

	private LocalDateTime completedAt;

	private Long lastProcessingTimeMs;

	private Double recordsPerSecond;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	public Long getFileSize() {
		return fileSize;
	}

	public void setFileSize(Long fileSize) {
		this.fileSize = fileSize;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

	public long getPassedRecords() {
		return passedRecords;
	}

	public void setPassedRecords(long passedRecords) {
		this.passedRecords = passedRecords;
	}

	public long getFailedRecords() {
		return failedRecords;
	}

	public void setFailedRecords(long failedRecords) {
		this.failedRecords = failedRecords;
	}

	public long getDuplicateRecords() {
		return duplicateRecords;
	}

	public void setDuplicateRecords(long duplicateRecords) {
		this.duplicateRecords = duplicateRecords;
	}

	public int getTotalChunks() {
		return totalChunks;
	}

	public void setTotalChunks(int totalChunks) {
		this.totalChunks = totalChunks;
	}

	public int getProcessedChunks() {
		return processedChunks;
	}

	public void setProcessedChunks(int processedChunks) {
		this.processedChunks = processedChunks;
	}

	public int getFailedChunks() {
		return failedChunks;
	}

	public void setFailedChunks(int failedChunks) {
		this.failedChunks = failedChunks;
	}

	public int getPendingChunks() {
		return pendingChunks;
	}

	public void setPendingChunks(int pendingChunks) {
		this.pendingChunks = pendingChunks;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public LocalDateTime getStartedAt() {
		return startedAt;
	}

	public void setStartedAt(LocalDateTime startedAt) {
		this.startedAt = startedAt;
	}

	public LocalDateTime getCompletedAt() {
		return completedAt;
	}

	public void setCompletedAt(LocalDateTime completedAt) {
		this.completedAt = completedAt;
	}

	public Long getLastProcessingTimeMs() {
		return lastProcessingTimeMs;
	}

	public void setLastProcessingTimeMs(Long lastProcessingTimeMs) {
		this.lastProcessingTimeMs = lastProcessingTimeMs;
	}

	public Double getRecordsPerSecond() {
		return recordsPerSecond;
	}

	public void setRecordsPerSecond(Double recordsPerSecond) {
		this.recordsPerSecond = recordsPerSecond;
	}

}
