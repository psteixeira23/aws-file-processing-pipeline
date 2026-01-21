package com.patrick.awsfileprocessing.infrastructure.adapter.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.patrick.awsfileprocessing.application.model.IdempotencyKey;
import com.patrick.awsfileprocessing.domain.JobId;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

class DynamoDbIdempotencyStoreAdapterTest {
  @Test
  void tryAcquireReturnsTrueWhenPutSucceeds() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    DynamoDbIdempotencyStoreAdapter adapter = new DynamoDbIdempotencyStoreAdapter(client, "table");
    IdempotencyKey key =
        IdempotencyKey.of(JobId.fromString("123e4567-e89b-12d3-a456-426614174000"), "abc");

    boolean acquired = adapter.tryAcquire(key);

    assertThat(acquired).isTrue();
    ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
    verify(client).putItem(captor.capture());
    PutItemRequest request = captor.getValue();
    assertThat(request.tableName()).isEqualTo("table");
    assertThat(request.conditionExpression()).isEqualTo("attribute_not_exists(jobId)");
    assertThat(request.item()).containsKeys("jobId", "status", "checksum");
  }

  @Test
  void tryAcquireReturnsFalseWhenConditionalFails() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    doThrow(ConditionalCheckFailedException.builder().message("exists").build())
        .when(client)
        .putItem(any(PutItemRequest.class));

    DynamoDbIdempotencyStoreAdapter adapter = new DynamoDbIdempotencyStoreAdapter(client, "table");
    IdempotencyKey key =
        IdempotencyKey.of(JobId.fromString("123e4567-e89b-12d3-a456-426614174000"), null);

    assertThat(adapter.tryAcquire(key)).isFalse();
  }

  @Test
  void isCompletedChecksStatusAndChecksum() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    DynamoDbIdempotencyStoreAdapter adapter = new DynamoDbIdempotencyStoreAdapter(client, "table");
    JobId jobId = JobId.fromString("123e4567-e89b-12d3-a456-426614174000");
    IdempotencyKey key = IdempotencyKey.of(jobId, "abc");

    Map<String, AttributeValue> item = new HashMap<>();
    item.put("status", AttributeValue.builder().s("COMPLETED").build());
    item.put("checksum", AttributeValue.builder().s("abc").build());
    when(client.getItem(any(GetItemRequest.class)))
        .thenReturn(GetItemResponse.builder().item(item).build());

    assertThat(adapter.isCompleted(key)).isTrue();
    assertThat(adapter.isCompleted(IdempotencyKey.of(jobId, "other"))).isFalse();
  }

  @Test
  void isCompletedReturnsFalseWhenItemMissing() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

    DynamoDbIdempotencyStoreAdapter adapter = new DynamoDbIdempotencyStoreAdapter(client, "table");

    boolean completed =
        adapter.isCompleted(
            IdempotencyKey.of(JobId.fromString("123e4567-e89b-12d3-a456-426614174000"), "abc"));

    assertThat(completed).isFalse();
  }

  @Test
  void markCompletedUpdatesItem() {
    DynamoDbClient client = mock(DynamoDbClient.class);
    DynamoDbIdempotencyStoreAdapter adapter = new DynamoDbIdempotencyStoreAdapter(client, "table");
    IdempotencyKey key =
        IdempotencyKey.of(JobId.fromString("123e4567-e89b-12d3-a456-426614174000"), "abc");

    adapter.markCompleted(key, new S3Location("out-bucket", "out-key"), "abc");

    ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
    verify(client).updateItem(captor.capture());
    UpdateItemRequest request = captor.getValue();
    assertThat(request.tableName()).isEqualTo("table");
    assertThat(request.updateExpression())
        .contains("#status")
        .contains("checksum")
        .contains("outputBucket")
        .contains("outputKey");
    assertThat(request.expressionAttributeNames()).containsEntry("#status", "status");
    assertThat(request.expressionAttributeValues())
        .containsKeys(":status", ":checksum", ":outBucket", ":outKey");
  }
}
