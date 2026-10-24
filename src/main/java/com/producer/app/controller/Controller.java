package com.producer.app.controller;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/batch")
public class Controller {

	private static final Logger log = LoggerFactory.getLogger(Controller.class);

	private final JobOperator jobOperator;
	private final JobRepository jobRepository;
	private final Job patientJob;

	public Controller(JobOperator jobOperator, JobRepository jobRepository, Job patientJob) {
		this.jobOperator = jobOperator;
		this.jobRepository = jobRepository;
		this.patientJob = patientJob;
	}

	@Scheduled(fixedRate = 600000)
	public void startOnSchedule() throws Exception {
		Set<JobExecution> running = jobRepository.findRunningJobExecutions(patientJob.getName());
		if (!running.isEmpty()) {
			log.info("patientJob already running, skip scheduled start, executions={}", running.size());
			return;
		}
		start();
	}

	@GetMapping("/start")
	public ResponseEntity<Map<String, Object>> start() throws Exception {
		JobExecution execution = jobOperator.start(patientJob,
				new JobParametersBuilder().addLong("run.id",
						System.currentTimeMillis()).toJobParameters());
		return ResponseEntity.ok(Map.of("jobExecutionId",
				execution.getId(), "status", execution.getStatus().toString()));
	}

}
