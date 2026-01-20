package com.patrick.awsfileprocessing.application.usecase;

import static org.assertj.core.api.Assertions.assertThat;

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

class ProcessJobUseCaseInProgressTest {
  @Test
  void returnsEarlyWhenAnotherWorkerOwnsLock() {
    TrackingObjectStoragePort storagePort = new TrackingObjectStoragePort();
    IdempotencyStorePort idempotencyStore = new InProgressIdempotencyStore();

    ProcessJobUseCase useCase =
        new ProcessJobUseCase(
            storagePort,
            idempotencyStore,
            new NoopMetrics(),
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

    useCase.process(message);

    assertThat(storagePort.headCalls).isZero();
    assertThat(storagePort.getCalls).isZero();
    assertThat(storagePort.putCalls).isZero();
  }

  private static final class InProgressIdempotencyStore implements IdempotencyStorePort {
    @Override
    public boolean tryAcquire(IdempotencyKey key) {
      return false;
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

  private static final class NoopMetrics implements MetricsPort {
    @Override
    public void incrementCounter(String name, Map<String, String> tags) {}

    @Override
    public void recordTiming(String name, long milliseconds, Map<String, String> tags) {}
  }

  private static final class TrackingObjectStoragePort implements ObjectStoragePort {
    private int headCalls;
    private int getCalls;
    private int putCalls;

    @Override
    public InputStream getObjectStream(S3Location location) {
      getCalls++;
      throw new AssertionError("getObjectStream should not be called");
    }

    @Override
    public void putObject(S3Location location, byte[] bytes, String contentType) {
      putCalls++;
      throw new AssertionError("putObject should not be called");
    }

    @Override
    public ObjectMetadata headObject(S3Location location) {
      headCalls++;
      throw new AssertionError("headObject should not be called");
    }
  }
}
