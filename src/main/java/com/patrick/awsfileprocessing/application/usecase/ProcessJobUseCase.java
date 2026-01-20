package com.patrick.awsfileprocessing.application.usecase;

import com.patrick.awsfileprocessing.application.model.IdempotencyKey;
import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.application.model.ObjectMetadata;
import com.patrick.awsfileprocessing.application.port.ClockPort;
import com.patrick.awsfileprocessing.application.port.IdempotencyStorePort;
import com.patrick.awsfileprocessing.application.port.MetricsPort;
import com.patrick.awsfileprocessing.application.port.ObjectStoragePort;
import com.patrick.awsfileprocessing.application.service.CsvProcessingSummary;
import com.patrick.awsfileprocessing.application.service.CsvStreamProcessor;
import com.patrick.awsfileprocessing.common.JsonMapper;
import com.patrick.awsfileprocessing.domain.JobId;
import com.patrick.awsfileprocessing.domain.JobStatus;
import com.patrick.awsfileprocessing.domain.ProcessingResult;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public final class ProcessJobUseCase {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessJobUseCase.class);

  private final ObjectStoragePort objectStoragePort;
  private final IdempotencyStorePort idempotencyStorePort;
  private final MetricsPort metricsPort;
  private final ClockPort clockPort;
  private final CsvStreamProcessor csvStreamProcessor;
  private final boolean excludeHeader;

  public ProcessJobUseCase(
      ObjectStoragePort objectStoragePort,
      IdempotencyStorePort idempotencyStorePort,
      MetricsPort metricsPort,
      ClockPort clockPort,
      CsvStreamProcessor csvStreamProcessor,
      boolean excludeHeader) {
    this.objectStoragePort =
        Objects.requireNonNull(objectStoragePort, "objectStoragePort must not be null");
    this.idempotencyStorePort =
        Objects.requireNonNull(idempotencyStorePort, "idempotencyStorePort must not be null");
    this.metricsPort = Objects.requireNonNull(metricsPort, "metricsPort must not be null");
    this.clockPort = Objects.requireNonNull(clockPort, "clockPort must not be null");
    this.csvStreamProcessor =
        Objects.requireNonNull(csvStreamProcessor, "csvStreamProcessor must not be null");
    this.excludeHeader = excludeHeader;
  }

  public void process(JobMessage message) {
    Objects.requireNonNull(message, "message must not be null");
    JobId jobId = JobId.fromString(message.jobId());
    S3Location inputLocation = new S3Location(message.inputBucket(), message.inputKey());
    S3Location outputLocation = new S3Location(message.outputBucket(), message.outputKey());

    IdempotencyKey idempotencyKey = IdempotencyKey.of(jobId, message.checksum());

    MDC.put("jobId", jobId.asString());
    MDC.put("s3Key", inputLocation.key());
    try {
      if (idempotencyStorePort.isCompleted(idempotencyKey)) {
        LOGGER.info(
            "job_already_completed jobId={} inputBucket={} inputKey={} outputBucket={}"
                + " outputKey={}",
            jobId.asString(),
            inputLocation.bucket(),
            inputLocation.key(),
            outputLocation.bucket(),
            outputLocation.key());
        return;
      }

      if (!idempotencyStorePort.tryAcquire(idempotencyKey)) {
        LOGGER.info(
            "job_already_in_progress jobId={} inputBucket={} inputKey={}",
            jobId.asString(),
            inputLocation.bucket(),
            inputLocation.key());
        return;
      }

      Instant startedAt = clockPort.now();
      ObjectMetadata metadata = objectStoragePort.headObject(inputLocation);

      CsvProcessingSummary summary;
      try (InputStream inputStream = objectStoragePort.getObjectStream(inputLocation)) {
        summary = csvStreamProcessor.process(inputStream, excludeHeader);
      }

      Instant finishedAt = clockPort.now();
      long processingTimeMs = Duration.between(startedAt, finishedAt).toMillis();

      ProcessingResult result =
          new ProcessingResult(
              jobId.asString(),
              inputLocation.bucket(),
              inputLocation.key(),
              outputLocation.bucket(),
              outputLocation.key(),
              summary.totalLines(),
              metadata.sizeBytes(),
              summary.checksum(),
              startedAt,
              finishedAt,
              processingTimeMs,
              JobStatus.SUCCEEDED);

      byte[] payload = JsonMapper.writeBytes(result);
      objectStoragePort.putObject(outputLocation, payload, "application/json");

      IdempotencyKey completedKey = IdempotencyKey.of(jobId, summary.checksum());
      idempotencyStorePort.markCompleted(completedKey, outputLocation, summary.checksum());

      metricsPort.incrementCounter("jobs_processed", Map.of("status", "success"));
      metricsPort.recordTiming(
          "job_processing_time_ms", processingTimeMs, Map.of("status", "success"));

      LOGGER.info(
          "job_processed jobId={} totalLines={} fileSizeBytes={} checksum={} processingTimeMs={}"
              + " outputBucket={} outputKey={}",
          jobId.asString(),
          summary.totalLines(),
          metadata.sizeBytes(),
          summary.checksum(),
          processingTimeMs,
          outputLocation.bucket(),
          outputLocation.key());
    } catch (RuntimeException | Error e) {
      metricsPort.incrementCounter("jobs_processed", Map.of("status", "failure"));
      LOGGER.error(
          "job_processing_failed jobId={} errorType={} message={}",
          jobId.asString(),
          e.getClass().getSimpleName(),
          e.getMessage(),
          e);
      throw e;
    } catch (Exception e) {
      metricsPort.incrementCounter("jobs_processed", Map.of("status", "failure"));
      LOGGER.error(
          "job_processing_failed jobId={} errorType={} message={}",
          jobId.asString(),
          e.getClass().getSimpleName(),
          e.getMessage(),
          e);
      throw new IllegalStateException("Job processing failed", e);
    } finally {
      MDC.clear();
    }
  }
}
