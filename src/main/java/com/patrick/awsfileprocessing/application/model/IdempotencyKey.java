package com.patrick.awsfileprocessing.application.model;

import com.patrick.awsfileprocessing.domain.JobId;
import java.util.Objects;

public record IdempotencyKey(JobId jobId, String checksum) {
  public IdempotencyKey {
    Objects.requireNonNull(jobId, "jobId must not be null");
  }

  public static IdempotencyKey of(JobId jobId, String checksum) {
    return new IdempotencyKey(jobId, checksum);
  }

  public String asKeyString() {
    if (checksum == null || checksum.isBlank()) {
      return jobId.asString();
    }
    return jobId.asString() + ":" + checksum;
  }
}
