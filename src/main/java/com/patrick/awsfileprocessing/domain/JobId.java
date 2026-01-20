package com.patrick.awsfileprocessing.domain;

import java.util.Objects;
import java.util.UUID;

public final class JobId {
  private final UUID value;

  private JobId(UUID value) {
    this.value = Objects.requireNonNull(value, "value must not be null");
  }

  public static JobId newId() {
    return new JobId(UUID.randomUUID());
  }

  public static JobId fromString(String value) {
    Objects.requireNonNull(value, "value must not be null");
    return new JobId(UUID.fromString(value));
  }

  public UUID value() {
    return value;
  }

  public String asString() {
    return value.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof JobId jobId)) {
      return false;
    }
    return value.equals(jobId.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
