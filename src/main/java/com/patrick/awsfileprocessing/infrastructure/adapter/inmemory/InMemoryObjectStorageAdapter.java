package com.patrick.awsfileprocessing.infrastructure.adapter.inmemory;

import com.patrick.awsfileprocessing.application.model.ObjectMetadata;
import com.patrick.awsfileprocessing.application.port.ObjectStoragePort;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryObjectStorageAdapter implements ObjectStoragePort {
  private final Map<S3Location, StoredObject> storage = new ConcurrentHashMap<>();

  @Override
  public InputStream getObjectStream(S3Location location) {
    StoredObject stored = getStored(location);
    return new ByteArrayInputStream(Arrays.copyOf(stored.bytes, stored.bytes.length));
  }

  @Override
  public void putObject(S3Location location, byte[] bytes, String contentType) {
    Objects.requireNonNull(location, "location must not be null");
    Objects.requireNonNull(bytes, "bytes must not be null");
    storage.put(location, new StoredObject(Arrays.copyOf(bytes, bytes.length)));
  }

  @Override
  public ObjectMetadata headObject(S3Location location) {
    StoredObject stored = getStored(location);
    return new ObjectMetadata(stored.bytes.length);
  }

  private StoredObject getStored(S3Location location) {
    Objects.requireNonNull(location, "location must not be null");
    StoredObject stored = storage.get(location);
    if (stored == null) {
      throw new IllegalArgumentException(
          "Object not found: " + location.bucket() + "/" + location.key());
    }
    return stored;
  }

  private static final class StoredObject {
    private final byte[] bytes;

    private StoredObject(byte[] bytes) {
      this.bytes = bytes;
    }
  }
}
