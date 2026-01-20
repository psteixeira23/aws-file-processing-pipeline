package com.patrick.awsfileprocessing.application.usecase;

import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.application.port.ClockPort;
import com.patrick.awsfileprocessing.application.port.JobQueuePort;
import com.patrick.awsfileprocessing.domain.JobId;
import com.patrick.awsfileprocessing.domain.OutputKeyConvention;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CreateJobUseCase {
  private static final Logger LOGGER = LoggerFactory.getLogger(CreateJobUseCase.class);

  private final JobQueuePort jobQueuePort;
  private final ClockPort clockPort;
  private final String outputBucket;

  public CreateJobUseCase(JobQueuePort jobQueuePort, ClockPort clockPort, String outputBucket) {
    this.jobQueuePort = Objects.requireNonNull(jobQueuePort, "jobQueuePort must not be null");
    this.clockPort = Objects.requireNonNull(clockPort, "clockPort must not be null");
    this.outputBucket = requireNonBlank(outputBucket, "outputBucket");
  }

  public JobId createJob(S3Location inputLocation) {
    Objects.requireNonNull(inputLocation, "inputLocation must not be null");
    validateCsv(inputLocation.key());

    JobId jobId = JobId.newId();
    String outputKey = OutputKeyConvention.outputKeyFor(jobId);
    Instant createdAt = clockPort.now();

    JobMessage message =
        new JobMessage(
            jobId.asString(),
            inputLocation.bucket(),
            inputLocation.key(),
            outputBucket,
            outputKey,
            createdAt,
            null);

    jobQueuePort.sendJob(message);
    LOGGER.info(
        "job_created jobId={} inputBucket={} inputKey={} outputBucket={} outputKey={} createdAt={}",
        jobId.asString(),
        inputLocation.bucket(),
        inputLocation.key(),
        outputBucket,
        outputKey,
        createdAt);
    return jobId;
  }

  private void validateCsv(String key) {
    String normalized = key.toLowerCase(Locale.ROOT);
    if (!normalized.endsWith(".csv")) {
      throw new IllegalArgumentException("Unsupported file type: " + key);
    }
  }

  private String requireNonBlank(String value, String field) {
    Objects.requireNonNull(value, field + " must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
