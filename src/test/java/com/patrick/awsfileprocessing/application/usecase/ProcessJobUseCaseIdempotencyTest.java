package com.patrick.awsfileprocessing.application.usecase;

import static org.assertj.core.api.Assertions.assertThat;

import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.application.model.ObjectMetadata;
import com.patrick.awsfileprocessing.application.port.ObjectStoragePort;
import com.patrick.awsfileprocessing.application.service.CsvStreamProcessor;
import com.patrick.awsfileprocessing.domain.S3Location;
import com.patrick.awsfileprocessing.infrastructure.adapter.NoopMetricsAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.SystemClockAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.inmemory.InMemoryIdempotencyStoreAdapter;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProcessJobUseCaseIdempotencyTest {
  @Test
  void skipsSecondProcessingWhenAlreadyCompleted() {
    CountingObjectStoragePort storagePort = new CountingObjectStoragePort();
    InMemoryIdempotencyStoreAdapter idempotencyStore = new InMemoryIdempotencyStoreAdapter();

    S3Location input = new S3Location("input-bucket", "file.csv");
    storagePort.seedObject(input, "h1,h2\n1,2\n".getBytes(StandardCharsets.UTF_8));

    JobMessage message =
        new JobMessage(
            "123e4567-e89b-12d3-a456-426614174000",
            input.bucket(),
            input.key(),
            "output-bucket",
            "output/123e4567-e89b-12d3-a456-426614174000.json",
            Instant.parse("2024-01-01T10:00:00Z"),
            null);

    ProcessJobUseCase useCase =
        new ProcessJobUseCase(
            storagePort,
            idempotencyStore,
            new NoopMetricsAdapter(),
            new SystemClockAdapter(),
            new CsvStreamProcessor(),
            false);

    useCase.process(message);
    useCase.process(message);

    assertThat(storagePort.putCount()).isEqualTo(1);
  }

  private static final class CountingObjectStoragePort implements ObjectStoragePort {
    private final Map<S3Location, byte[]> objects = new HashMap<>();
    private int putCount;

    @Override
    public InputStream getObjectStream(S3Location location) {
      return new ByteArrayInputStream(objects.get(location));
    }

    @Override
    public void putObject(S3Location location, byte[] bytes, String contentType) {
      objects.put(location, bytes);
      putCount++;
    }

    @Override
    public ObjectMetadata headObject(S3Location location) {
      byte[] bytes = objects.get(location);
      return new ObjectMetadata(bytes.length);
    }

    private void seedObject(S3Location location, byte[] bytes) {
      objects.put(location, bytes);
    }

    private int putCount() {
      return putCount;
    }
  }
}
