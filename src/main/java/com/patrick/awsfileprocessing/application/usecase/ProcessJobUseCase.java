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
  private static final String METRIC_JOBS_PROCESSED = "jobs_processed";
  private static final String METRIC_PROCESSING_TIME = "job_processing_time_ms";
  private static final String OUTPUT_CONTENT_TYPE = "application/json";
  private static final String TAG_STATUS = "status";
  private static final String STATUS_SUCCESS = "success";
  private static final String STATUS_FAILURE = "failure";
  private static final Map<String, String> SUCCESS_TAGS = Map.of(TAG_STATUS, STATUS_SUCCESS);
  private static final Map<String, String> FAILURE_TAGS = Map.of(TAG_STATUS, STATUS_FAILURE);

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
    ProcessingContext context = buildContext(message);
    MDC.put("jobId", context.jobId().asString());
    MDC.put("s3Key", context.inputLocation().key());
    try {
      processInternal(context);
    } catch (Exception e) {
      recordFailure(context.jobId(), e);
      if (e instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new IllegalStateException("Job processing failed", e);
    } finally {
      MDC.clear();
    }
  }

  private ProcessingContext buildContext(JobMessage message) {
    Objects.requireNonNull(message, "message must not be null");
    JobId jobId = JobId.fromString(message.jobId());
    S3Location inputLocation = new S3Location(message.inputBucket(), message.inputKey());
    S3Location outputLocation = new S3Location(message.outputBucket(), message.outputKey());
    IdempotencyKey idempotencyKey = IdempotencyKey.of(jobId, message.checksum());
    return new ProcessingContext(jobId, inputLocation, outputLocation, idempotencyKey);
  }

  private void processInternal(ProcessingContext context) throws Exception {
    if (shouldSkip(context)) {
      return;
    }

    Instant startedAt = clockPort.now();
    ObjectMetadata metadata = objectStoragePort.headObject(context.inputLocation());
    CsvProcessingSummary summary = processCsv(context.inputLocation());
    Instant finishedAt = clockPort.now();
    long processingTimeMs = Duration.between(startedAt, finishedAt).toMillis();

    ProcessingResult result =
        new ProcessingResult(
            context.jobId().asString(),
            context.inputLocation().bucket(),
            context.inputLocation().key(),
            context.outputLocation().bucket(),
            context.outputLocation().key(),
            summary.totalLines(),
            metadata.sizeBytes(),
            summary.checksum(),
            startedAt,
            finishedAt,
            processingTimeMs,
            JobStatus.SUCCEEDED);

    writeResult(context.outputLocation(), result);
    markCompleted(context.jobId(), context.outputLocation(), summary);
    recordSuccess(processingTimeMs);
    logSuccess(context, summary, metadata, processingTimeMs);
  }

  private boolean shouldSkip(ProcessingContext context) {
    if (idempotencyStorePort.isCompleted(context.idempotencyKey())) {
      LOGGER.info(
          "job_already_completed jobId={} inputBucket={} inputKey={} outputBucket={} outputKey={}",
          context.jobId().asString(),
          context.inputLocation().bucket(),
          context.inputLocation().key(),
          context.outputLocation().bucket(),
          context.outputLocation().key());
      return true;
    }

    if (!idempotencyStorePort.tryAcquire(context.idempotencyKey())) {
      LOGGER.info(
          "job_already_in_progress jobId={} inputBucket={} inputKey={}",
          context.jobId().asString(),
          context.inputLocation().bucket(),
          context.inputLocation().key());
      return true;
    }

    return false;
  }

  private CsvProcessingSummary processCsv(S3Location inputLocation) throws Exception {
    try (InputStream inputStream = objectStoragePort.getObjectStream(inputLocation)) {
      return csvStreamProcessor.process(inputStream, excludeHeader);
    }
  }

  private void writeResult(S3Location outputLocation, ProcessingResult result) {
    byte[] payload = JsonMapper.writeBytes(result);
    objectStoragePort.putObject(outputLocation, payload, OUTPUT_CONTENT_TYPE);
  }

  private void markCompleted(JobId jobId, S3Location outputLocation, CsvProcessingSummary summary) {
    IdempotencyKey completedKey = IdempotencyKey.of(jobId, summary.checksum());
    idempotencyStorePort.markCompleted(completedKey, outputLocation, summary.checksum());
  }

  private void recordSuccess(long processingTimeMs) {
    metricsPort.incrementCounter(METRIC_JOBS_PROCESSED, SUCCESS_TAGS);
    metricsPort.recordTiming(METRIC_PROCESSING_TIME, processingTimeMs, SUCCESS_TAGS);
  }

  private void recordFailure(JobId jobId, Exception exception) {
    metricsPort.incrementCounter(METRIC_JOBS_PROCESSED, FAILURE_TAGS);
    LOGGER.error(
        "job_processing_failed jobId={} errorType={} message={}",
        jobId.asString(),
        exception.getClass().getSimpleName(),
        exception.getMessage(),
        exception);
  }

  private void logSuccess(
      ProcessingContext context,
      CsvProcessingSummary summary,
      ObjectMetadata metadata,
      long processingTimeMs) {
    LOGGER.info(
        "job_processed jobId={} totalLines={} fileSizeBytes={} checksum={} processingTimeMs={}"
            + " outputBucket={} outputKey={}",
        context.jobId().asString(),
        summary.totalLines(),
        metadata.sizeBytes(),
        summary.checksum(),
        processingTimeMs,
        context.outputLocation().bucket(),
        context.outputLocation().key());
  }

  private record ProcessingContext(
      JobId jobId,
      S3Location inputLocation,
      S3Location outputLocation,
      IdempotencyKey idempotencyKey) {}
}
