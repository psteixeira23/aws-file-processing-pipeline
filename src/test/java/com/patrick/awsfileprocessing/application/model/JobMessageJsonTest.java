package com.patrick.awsfileprocessing.application.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.patrick.awsfileprocessing.common.JsonMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class JobMessageJsonTest {
  @Test
  void serializesAndDeserializes() {
    JobMessage message =
        new JobMessage(
            "123e4567-e89b-12d3-a456-426614174000",
            "input-bucket",
            "file.csv",
            "output-bucket",
            "output/123e4567-e89b-12d3-a456-426614174000.json",
            Instant.parse("2024-01-01T10:00:00Z"),
            "abc123");

    String json = JsonMapper.writeString(message);
    JobMessage decoded = JsonMapper.read(json, JobMessage.class);

    assertThat(decoded).isEqualTo(message);
  }
}
