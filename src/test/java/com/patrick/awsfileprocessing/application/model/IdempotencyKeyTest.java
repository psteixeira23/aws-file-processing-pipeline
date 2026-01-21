package com.patrick.awsfileprocessing.application.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.patrick.awsfileprocessing.domain.JobId;
import org.junit.jupiter.api.Test;

class IdempotencyKeyTest {
  @Test
  void includesChecksumWhenProvided() {
    JobId jobId = JobId.fromString("123e4567-e89b-12d3-a456-426614174000");

    IdempotencyKey key = IdempotencyKey.of(jobId, "abc123");

    assertThat(key.asKeyString()).isEqualTo(jobId.asString() + ":abc123");
  }

  @Test
  void omitsChecksumWhenBlank() {
    JobId jobId = JobId.fromString("123e4567-e89b-12d3-a456-426614174000");

    IdempotencyKey key = IdempotencyKey.of(jobId, " ");

    assertThat(key.asKeyString()).isEqualTo(jobId.asString());
  }

  @Test
  void rejectsNullJobId() {
    assertThatThrownBy(() -> IdempotencyKey.of(null, "checksum"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("jobId");
  }
}
