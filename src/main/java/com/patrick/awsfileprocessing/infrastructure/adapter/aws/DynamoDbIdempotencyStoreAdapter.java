package com.patrick.awsfileprocessing.infrastructure.adapter.aws;

import com.patrick.awsfileprocessing.application.model.IdempotencyKey;
import com.patrick.awsfileprocessing.application.port.IdempotencyStorePort;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public final class DynamoDbIdempotencyStoreAdapter implements IdempotencyStorePort {
  private static final String STATUS_IN_PROGRESS = "IN_PROGRESS";
  private static final String STATUS_COMPLETED = "COMPLETED";

  private final DynamoDbClient dynamoDbClient;
  private final String tableName;

  public DynamoDbIdempotencyStoreAdapter(DynamoDbClient dynamoDbClient, String tableName) {
    this.dynamoDbClient = Objects.requireNonNull(dynamoDbClient, "dynamoDbClient must not be null");
    this.tableName = requireNonBlank(tableName, "tableName");
  }

  @Override
  public boolean tryAcquire(IdempotencyKey key) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("jobId", AttributeValue.builder().s(key.jobId().asString()).build());
    item.put("status", AttributeValue.builder().s(STATUS_IN_PROGRESS).build());
    if (key.checksum() != null && !key.checksum().isBlank()) {
      item.put("checksum", AttributeValue.builder().s(key.checksum()).build());
    }
    PutItemRequest request =
        PutItemRequest.builder()
            .tableName(tableName)
            .item(item)
            .conditionExpression("attribute_not_exists(jobId)")
            .build();
    try {
      dynamoDbClient.putItem(request);
      return true;
    } catch (ConditionalCheckFailedException e) {
      return false;
    }
  }

  @Override
  public boolean isCompleted(IdempotencyKey key) {
    GetItemRequest request =
        GetItemRequest.builder()
            .tableName(tableName)
            .key(Map.of("jobId", AttributeValue.builder().s(key.jobId().asString()).build()))
            .build();
    GetItemResponse response = dynamoDbClient.getItem(request);
    if (!response.hasItem()) {
      return false;
    }
    Map<String, AttributeValue> item = response.item();
    AttributeValue statusValue = item.get("status");
    if (statusValue == null || !STATUS_COMPLETED.equals(statusValue.s())) {
      return false;
    }
    if (key.checksum() == null || key.checksum().isBlank()) {
      return true;
    }
    AttributeValue checksumValue = item.get("checksum");
    return checksumValue != null && key.checksum().equals(checksumValue.s());
  }

  @Override
  public void markCompleted(IdempotencyKey key, S3Location outputLocation, String checksum) {
    Map<String, AttributeValue> keyMap =
        Map.of("jobId", AttributeValue.builder().s(key.jobId().asString()).build());
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":status", AttributeValue.builder().s(STATUS_COMPLETED).build());
    values.put(":checksum", AttributeValue.builder().s(checksum).build());
    values.put(":outBucket", AttributeValue.builder().s(outputLocation.bucket()).build());
    values.put(":outKey", AttributeValue.builder().s(outputLocation.key()).build());

    UpdateItemRequest request =
        UpdateItemRequest.builder()
            .tableName(tableName)
            .key(keyMap)
            .updateExpression(
                "SET #status = :status, checksum = :checksum, outputBucket = :outBucket, outputKey"
                    + " = :outKey")
            .expressionAttributeNames(Map.of("#status", "status"))
            .expressionAttributeValues(values)
            .build();
    dynamoDbClient.updateItem(request);
  }

  private String requireNonBlank(String value, String field) {
    Objects.requireNonNull(value, field + " must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
