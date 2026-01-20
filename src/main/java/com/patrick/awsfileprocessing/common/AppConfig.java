package com.patrick.awsfileprocessing.common;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public record AppConfig(
    String inputBucket,
    String outputBucket,
    String jobQueueUrl,
    String awsRegion,
    IdempotencyMode idempotencyMode,
    String ddbTableName,
    boolean excludeHeader) {
  public static AppConfig fromEnv() {
    return fromEnv(System.getenv());
  }

  public static AppConfig fromEnv(Map<String, String> env) {
    String inputBucket = env.get("INPUT_BUCKET");
    String outputBucket = env.get("OUTPUT_BUCKET");
    String jobQueueUrl = env.get("JOB_QUEUE_URL");
    String awsRegion = env.get("AWS_REGION");
    String idpMode = env.getOrDefault("IDP_MODE", "IN_MEMORY");
    String ddbTableName = env.get("DDB_TABLE_NAME");
    boolean excludeHeader = Boolean.parseBoolean(env.getOrDefault("EXCLUDE_HEADER", "false"));
    return new AppConfig(
        inputBucket,
        outputBucket,
        jobQueueUrl,
        awsRegion,
        IdempotencyMode.fromString(idpMode),
        ddbTableName,
        excludeHeader);
  }

  public AppConfig requireOutputBucket() {
    requireNonBlank(outputBucket, "OUTPUT_BUCKET");
    return this;
  }

  public AppConfig requireJobQueueUrl() {
    requireNonBlank(jobQueueUrl, "JOB_QUEUE_URL");
    return this;
  }

  public AppConfig requireAwsRegion() {
    requireNonBlank(awsRegion, "AWS_REGION");
    return this;
  }

  public AppConfig requireInputBucket() {
    requireNonBlank(inputBucket, "INPUT_BUCKET");
    return this;
  }

  public AppConfig requireDynamoTable() {
    if (idempotencyMode == IdempotencyMode.DYNAMODB) {
      requireNonBlank(ddbTableName, "DDB_TABLE_NAME");
    }
    return this;
  }

  private void requireNonBlank(String value, String field) {
    Objects.requireNonNull(value, field + " must be set");
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }

  public enum IdempotencyMode {
    IN_MEMORY,
    DYNAMODB;

    public static IdempotencyMode fromString(String value) {
      Objects.requireNonNull(value, "idp mode must not be null");
      String normalized = value.trim().toUpperCase(Locale.ROOT);
      return IdempotencyMode.valueOf(normalized);
    }
  }
}
