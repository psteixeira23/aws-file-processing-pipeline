package com.patrick.awsfileprocessing.infrastructure.adapter.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import com.patrick.awsfileprocessing.application.model.JobMessage;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class InMemoryJobQueueAdapterTest {
  @Test
  void storesAndPollsMessages() {
    InMemoryJobQueueAdapter adapter = new InMemoryJobQueueAdapter();
    JobMessage message =
        new JobMessage(
            "123e4567-e89b-12d3-a456-426614174000",
            "input-bucket",
            "input.csv",
            "output-bucket",
            "output/123e4567-e89b-12d3-a456-426614174000.json",
            Instant.parse("2024-01-01T00:00:00Z"),
            null);

    adapter.sendJob(message);

    assertThat(adapter.size()).isEqualTo(1);
    assertThat(adapter.poll()).isEqualTo(message);
    assertThat(adapter.size()).isZero();
  }
}
