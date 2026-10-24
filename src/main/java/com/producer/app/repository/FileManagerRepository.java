package com.producer.app.repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import com.producer.app.model.FileManager;

public interface FileManagerRepository extends JpaRepository<FileManager, Long> {

	@Query(value = "select * from filemanager where file_name = :fileName", nativeQuery = true)
	Optional<FileManager> findByFileName(@Param("fileName") String fileName);

	@Query(value = """
			select * from filemanager
			where status <> 'COMPLETED'
			  and (status = 'READY' or pending_chunks > 0)
			order by file_name
			""", nativeQuery = true)
	List<FileManager> findAssignable();

	@Modifying(flushAutomatically = true)
	@Transactional
	@Query(value = """
			update filemanager
			set processed_chunks = processed_chunks + 1,
				passed_records = passed_records + :passed,
				failed_records = failed_records + :failed,
				duplicate_records = duplicate_records + :duplicates,
				pending_chunks = case
					when total_chunks - (processed_chunks + 1) - failed_chunks < 0 then 0
					else total_chunks - (processed_chunks + 1) - failed_chunks
				end
			where file_name = :fileName
			""", nativeQuery = true)
	int incrementCounts(@Param("fileName") String fileName, @Param("passed") long passed, @Param("failed") long failed,
			@Param("duplicates") long duplicates);

	@Modifying(flushAutomatically = true)
	@Transactional
	@Query(value = """
			update filemanager
			set passed_records = passed_records + :passed,
				failed_records = failed_records + :failed,
				duplicate_records = duplicate_records + :duplicates
			where file_name = :fileName
			""", nativeQuery = true)
	int incrementRecovered(@Param("fileName") String fileName, @Param("passed") long passed,
			@Param("failed") long failed, @Param("duplicates") long duplicates);

	@Modifying(flushAutomatically = true)
	@Transactional
	@Query(value = """
			update filemanager
			set status = 'FAILED',
				processed = false,
				failed_chunks = failed_chunks + 1,
				pending_chunks = case
					when total_chunks - processed_chunks - (failed_chunks + 1) < 0 then 0
					else total_chunks - processed_chunks - (failed_chunks + 1)
				end,
				error_message = :errorMessage,
				last_processing_time_ms = :millis,
				records_per_second = :recordsPerSecond
			where file_name = :fileName
			""", nativeQuery = true)
	int markFailed(@Param("fileName") String fileName, @Param("errorMessage") String errorMessage,
			@Param("millis") Long millis, @Param("recordsPerSecond") Double recordsPerSecond);

	@Modifying(flushAutomatically = true)
	@Transactional
	@Query(value = """
			update filemanager
			set last_processing_time_ms = :millis,
				records_per_second = :recordsPerSecond
			where file_name = :fileName
			""", nativeQuery = true)
	int updateTiming(@Param("fileName") String fileName, @Param("millis") Long millis,
			@Param("recordsPerSecond") Double recordsPerSecond);

	@Modifying(flushAutomatically = true)
	@Transactional
	@Query(value = """
			update filemanager
			set status = 'COMPLETED',
				processed = true,
				pending_chunks = 0,
				processed_chunks = total_chunks,
				completed_at = :completedAt,
				error_message = null,
				last_processing_time_ms = :millis,
				records_per_second = :recordsPerSecond
			where file_name = :fileName
			  and status <> 'FAILED'
			""", nativeQuery = true)
	int markCompleted(@Param("fileName") String fileName, @Param("completedAt") LocalDateTime completedAt,
			@Param("millis") Long millis, @Param("recordsPerSecond") Double recordsPerSecond);

}
