package com.patrick.awsfileprocessing.infrastructure.adapter.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.patrick.awsfileprocessing.application.model.ObjectMetadata;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class InMemoryObjectStorageAdapterTest {
  @Test
  void storesAndReturnsObjects() throws Exception {
    InMemoryObjectStorageAdapter adapter = new InMemoryObjectStorageAdapter();
    S3Location location = new S3Location("bucket", "key.csv");
    byte[] payload = "data".getBytes(StandardCharsets.UTF_8);

    adapter.putObject(location, payload, "text/csv");

    try (InputStream inputStream = adapter.getObjectStream(location)) {
      assertThat(inputStream.readAllBytes()).isEqualTo(payload);
    }

    ObjectMetadata metadata = adapter.headObject(location);
    assertThat(metadata.sizeBytes()).isEqualTo(payload.length);
  }

  @Test
  void throwsWhenObjectMissing() {
    InMemoryObjectStorageAdapter adapter = new InMemoryObjectStorageAdapter();

    assertThatThrownBy(() -> adapter.getObjectStream(new S3Location("bucket", "missing")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Object not found");
  }
}
