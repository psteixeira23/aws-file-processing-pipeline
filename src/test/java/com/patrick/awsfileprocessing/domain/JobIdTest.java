package com.patrick.awsfileprocessing.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class JobIdTest {
  @Test
  void newIdGeneratesUuid() {
    JobId jobId = JobId.newId();

    UUID parsed = UUID.fromString(jobId.asString());

    assertThat(parsed).isEqualTo(jobId.value());
  }

  @Test
  void fromStringRejectsInvalidValue() {
    assertThatThrownBy(() -> JobId.fromString("not-a-uuid"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void equalsUsesUnderlyingUuid() {
    JobId first = JobId.fromString("123e4567-e89b-12d3-a456-426614174000");
    JobId second = JobId.fromString("123e4567-e89b-12d3-a456-426614174000");

    assertThat(first).isEqualTo(second);
  }
}
