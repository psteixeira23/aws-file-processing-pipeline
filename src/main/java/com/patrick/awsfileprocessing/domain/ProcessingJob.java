package com.patrick.awsfileprocessing.domain;

import java.time.Instant;
import java.util.Objects;

public record ProcessingJob(
    JobId jobId,
    S3Location inputLocation,
    S3Location outputLocation,
    String checksum,
    Instant createdAt,
    JobStatus status) {
  public ProcessingJob {
    Objects.requireNonNull(jobId, "jobId must not be null");
    Objects.requireNonNull(inputLocation, "inputLocation must not be null");
    Objects.requireNonNull(outputLocation, "outputLocation must not be null");
    Objects.requireNonNull(createdAt, "createdAt must not be null");
    Objects.requireNonNull(status, "status must not be null");
  }
}
