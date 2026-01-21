package com.patrick.awsfileprocessing.application.usecase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.patrick.awsfileprocessing.application.model.IdempotencyKey;
import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.application.model.ObjectMetadata;
import com.patrick.awsfileprocessing.application.port.IdempotencyStorePort;
import com.patrick.awsfileprocessing.application.port.MetricsPort;
import com.patrick.awsfileprocessing.application.port.ObjectStoragePort;
import com.patrick.awsfileprocessing.application.service.CsvStreamProcessor;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProcessJobUseCaseFailureTest {
  @Test
  void recordsFailureMetricsWhenProcessingFails() {
    FailingStoragePort storagePort = new FailingStoragePort();
    TestMetrics metrics = new TestMetrics();
    IdempotencyStorePort idempotencyStore = new AllowAllIdempotencyStore();

    ProcessJobUseCase useCase =
        new ProcessJobUseCase(
            storagePort,
            idempotencyStore,
            metrics,
            () -> Instant.parse("2024-01-01T10:00:00Z"),
            new CsvStreamProcessor(),
            false);

    JobMessage message =
        new JobMessage(
            "123e4567-e89b-12d3-a456-426614174000",
            "input-bucket",
            "input.csv",
            "output-bucket",
            "output/123e4567-e89b-12d3-a456-426614174000.json",
            Instant.parse("2024-01-01T10:00:00Z"),
            null);

    assertThatThrownBy(() -> useCase.process(message))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("boom");

    assertThat(metrics.incremented).isEqualTo(1);
    assertThat(metrics.lastName).isEqualTo("jobs_processed");
    assertThat(metrics.lastTags).containsEntry("status", "failure");
  }

  private static final class FailingStoragePort implements ObjectStoragePort {
    @Override
    public InputStream getObjectStream(S3Location location) {
      throw new IllegalStateException("boom");
    }

    @Override
    public void putObject(S3Location location, byte[] bytes, String contentType) {
      throw new AssertionError("putObject should not be called");
    }

    @Override
    public ObjectMetadata headObject(S3Location location) {
      return new ObjectMetadata(10L);
    }
  }

  private static final class AllowAllIdempotencyStore implements IdempotencyStorePort {
    @Override
    public boolean tryAcquire(IdempotencyKey key) {
      return true;
    }

    @Override
    public boolean isCompleted(IdempotencyKey key) {
      return false;
    }

    @Override
    public void markCompleted(IdempotencyKey key, S3Location outputLocation, String checksum) {
      throw new AssertionError("markCompleted should not be called");
    }
  }

  private static final class TestMetrics implements MetricsPort {
    private int incremented;
    private String lastName;
    private Map<String, String> lastTags;

    @Override
    public void incrementCounter(String name, Map<String, String> tags) {
      incremented++;
      lastName = name;
      lastTags = tags;
    }

    @Override
    public void recordTiming(String name, long milliseconds, Map<String, String> tags) {}
  }
}
