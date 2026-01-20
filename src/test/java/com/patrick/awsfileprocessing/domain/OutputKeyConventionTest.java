package com.patrick.awsfileprocessing.domain;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class OutputKeyConventionTest {
  @Test
  void buildsOutputKeyFromJobId() {
    JobId jobId = JobId.fromString("123e4567-e89b-12d3-a456-426614174000");

    String outputKey = OutputKeyConvention.outputKeyFor(jobId);

    assertThat(outputKey).isEqualTo("output/123e4567-e89b-12d3-a456-426614174000.json");
  }
}
