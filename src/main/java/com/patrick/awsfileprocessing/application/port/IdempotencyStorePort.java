package com.patrick.awsfileprocessing.application.port;

import com.patrick.awsfileprocessing.application.model.IdempotencyKey;
import com.patrick.awsfileprocessing.domain.S3Location;

public interface IdempotencyStorePort {
  boolean tryAcquire(IdempotencyKey key);

  boolean isCompleted(IdempotencyKey key);

  void markCompleted(IdempotencyKey key, S3Location outputLocation, String checksum);
}
