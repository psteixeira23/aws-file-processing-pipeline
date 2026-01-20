package com.patrick.awsfileprocessing.domain;

import java.time.Instant;
import java.util.Objects;

public record ProcessingResult(
    String jobId,
    String inputBucket,
    String inputKey,
    String outputBucket,
    String outputKey,
    long totalLines,
    long fileSizeBytes,
    String checksum,
    Instant startedAt,
    Instant finishedAt,
    long processingTimeMs,
    JobStatus status) {
  public ProcessingResult {
    requireNonBlank(jobId, "jobId");
    requireNonBlank(inputBucket, "inputBucket");
    requireNonBlank(inputKey, "inputKey");
    requireNonBlank(outputBucket, "outputBucket");
    requireNonBlank(outputKey, "outputKey");
    Objects.requireNonNull(checksum, "checksum must not be null");
    Objects.requireNonNull(startedAt, "startedAt must not be null");
    Objects.requireNonNull(finishedAt, "finishedAt must not be null");
    Objects.requireNonNull(status, "status must not be null");
  }

  private static void requireNonBlank(String value, String field) {
    Objects.requireNonNull(value, field + " must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }
}
