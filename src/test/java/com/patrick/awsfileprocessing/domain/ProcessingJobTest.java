package com.patrick.awsfileprocessing.domain;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class ProcessingJobTest {
  @Test
  void rejectsNullJobId() {
    assertThatThrownBy(
            () ->
                new ProcessingJob(
                    null,
                    new S3Location("input", "in.csv"),
                    new S3Location("output", "out.json"),
                    "checksum",
                    Instant.parse("2024-01-01T00:00:00Z"),
                    JobStatus.RECEIVED))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("jobId");
  }

  @Test
  void rejectsNullStatus() {
    assertThatThrownBy(
            () ->
                new ProcessingJob(
                    JobId.fromString("123e4567-e89b-12d3-a456-426614174000"),
                    new S3Location("input", "in.csv"),
                    new S3Location("output", "out.json"),
                    "checksum",
                    Instant.parse("2024-01-01T00:00:00Z"),
                    null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("status");
  }
}
