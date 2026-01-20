package com.patrick.awsfileprocessing.domain;

import java.util.Objects;

public record S3Location(String bucket, String key) {
  public S3Location {
    requireNonBlank(bucket, "bucket");
    requireNonBlank(key, "key");
  }

  private static void requireNonBlank(String value, String field) {
    Objects.requireNonNull(value, field + " must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }
}
