package com.patrick.awsfileprocessing.entrypoint.lambda;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.common.JsonMapper;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SqsWorkerLambdaTest {
  @Test
  void returnsEmptyFailuresForNullEvent() {
    SqsWorkerLambda handler = new SqsWorkerLambda(message -> {});

    SQSBatchResponse response = handler.handleRequest(null, null);

    assertThat(response.getBatchItemFailures()).isEmpty();
  }

  @Test
  void recordsFailureWhenMessageIsInvalid() {
    SQSEvent event = new SQSEvent();
    SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
    message.setMessageId("msg-1");
    message.setBody("not-json");
    event.setRecords(List.of(message));

    SqsWorkerLambda handler = new SqsWorkerLambda(payload -> {});

    SQSBatchResponse response = handler.handleRequest(event, null);

    assertThat(response.getBatchItemFailures())
        .extracting(SQSBatchResponse.BatchItemFailure::getItemIdentifier)
        .containsExactly("msg-1");
  }

  @Test
  void processesValidMessage() {
    AtomicReference<JobMessage> captured = new AtomicReference<>();
    SqsWorkerLambda handler = new SqsWorkerLambda(captured::set);

    JobMessage jobMessage =
        new JobMessage(
            "123e4567-e89b-12d3-a456-426614174000",
            "input-bucket",
            "input.csv",
            "output-bucket",
            "output/123e4567-e89b-12d3-a456-426614174000.json",
            Instant.parse("2024-01-01T00:00:00Z"),
            null);

    SQSEvent event = new SQSEvent();
    SQSEvent.SQSMessage record = new SQSEvent.SQSMessage();
    record.setMessageId("msg-2");
    record.setBody(JsonMapper.writeString(jobMessage));
    event.setRecords(List.of(record));

    SQSBatchResponse response = handler.handleRequest(event, null);

    assertThat(response.getBatchItemFailures()).isEmpty();
    assertThat(captured.get()).isEqualTo(jobMessage);
  }
}
