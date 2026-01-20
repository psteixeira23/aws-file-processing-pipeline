package com.patrick.awsfileprocessing.infrastructure.adapter.inmemory;

import com.patrick.awsfileprocessing.application.model.IdempotencyKey;
import com.patrick.awsfileprocessing.application.port.IdempotencyStorePort;
import com.patrick.awsfileprocessing.domain.JobId;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class InMemoryIdempotencyStoreAdapter implements IdempotencyStorePort {
  private final Map<JobId, Entry> entries = new ConcurrentHashMap<>();

  @Override
  public boolean tryAcquire(IdempotencyKey key) {
    Objects.requireNonNull(key, "key must not be null");
    AtomicBoolean acquired = new AtomicBoolean(false);
    entries.compute(
        key.jobId(),
        (jobId, existing) -> {
          if (existing == null) {
            acquired.set(true);
            return Entry.inProgress(key.checksum());
          }
          return existing;
        });
    return acquired.get();
  }

  @Override
  public boolean isCompleted(IdempotencyKey key) {
    Objects.requireNonNull(key, "key must not be null");
    Entry entry = entries.get(key.jobId());
    if (entry == null || entry.status != Status.COMPLETED) {
      return false;
    }
    if (key.checksum() == null || key.checksum().isBlank()) {
      return true;
    }
    return key.checksum().equals(entry.checksum);
  }

  @Override
  public void markCompleted(IdempotencyKey key, S3Location outputLocation, String checksum) {
    Objects.requireNonNull(key, "key must not be null");
    Objects.requireNonNull(outputLocation, "outputLocation must not be null");
    entries.put(key.jobId(), Entry.completed(checksum));
  }

  private enum Status {
    IN_PROGRESS,
    COMPLETED
  }

  private static final class Entry {
    private final Status status;
    private final String checksum;

    private Entry(Status status, String checksum) {
      this.status = status;
      this.checksum = checksum;
    }

    private static Entry inProgress(String checksum) {
      return new Entry(Status.IN_PROGRESS, checksum);
    }

    private static Entry completed(String checksum) {
      return new Entry(Status.COMPLETED, checksum);
    }
  }
}
