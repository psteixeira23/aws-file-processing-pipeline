package com.patrick.awsfileprocessing.entrypoint.lambda;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class S3EventPublisherLambdaTest {
  @Test
  void skipsWhenEventIsNull() {
    List<S3Location> captured = new ArrayList<>();
    S3EventPublisherLambda handler = new S3EventPublisherLambda(captured::add);

    handler.handleRequest(null, null);

    assertThat(captured).isEmpty();
  }

  @Test
  void publishesJobForEachRecord() {
    List<S3Location> captured = new ArrayList<>();
    S3EventPublisherLambda handler = new S3EventPublisherLambda(captured::add);

    S3Event event = s3Event("input-bucket", List.of("first.csv", "second.csv"));

    handler.handleRequest(event, null);

    assertThat(captured)
        .containsExactly(
            new S3Location("input-bucket", "first.csv"),
            new S3Location("input-bucket", "second.csv"));
  }

  private static S3Event s3Event(String bucket, List<String> keys) {
    List<S3EventNotification.S3EventNotificationRecord> records = new ArrayList<>();
    S3EventNotification.UserIdentityEntity identity =
        new S3EventNotification.UserIdentityEntity("principal");

    for (String key : keys) {
      S3EventNotification.S3ObjectEntity objectEntity =
          new S3EventNotification.S3ObjectEntity(key, 1L, "etag", "version");
      S3EventNotification.S3BucketEntity bucketEntity =
          new S3EventNotification.S3BucketEntity(bucket, identity, "arn:aws:s3:::" + bucket);
      S3EventNotification.S3Entity s3Entity =
          new S3EventNotification.S3Entity("config", bucketEntity, objectEntity, "1.0");
      S3EventNotification.S3EventNotificationRecord record =
          new S3EventNotification.S3EventNotificationRecord(
              "us-east-1",
              "ObjectCreated:Put",
              "aws:s3",
              "2024-01-01T00:00:00.000Z",
              "2.1",
              null,
              null,
              s3Entity,
              identity);
      records.add(record);
    }

    return new S3Event(records);
  }
}
