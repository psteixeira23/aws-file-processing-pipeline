package com.patrick.awsfileprocessing.infrastructure.adapter.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.common.JsonMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

class SqsJobQueueAdapterTest {
  @Test
  void sendJobPublishesSerializedMessage() {
    SqsClient client = mock(SqsClient.class);
    SqsJobQueueAdapter adapter = new SqsJobQueueAdapter(client, "queue-url");
    JobMessage message =
        new JobMessage(
            "123e4567-e89b-12d3-a456-426614174000",
            "input-bucket",
            "input.csv",
            "output-bucket",
            "output/123e4567-e89b-12d3-a456-426614174000.json",
            Instant.parse("2024-01-01T00:00:00Z"),
            null);

    adapter.sendJob(message);

    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(client).sendMessage(captor.capture());
    SendMessageRequest request = captor.getValue();
    assertThat(request.queueUrl()).isEqualTo("queue-url");

    JobMessage decoded = JsonMapper.read(request.messageBody(), JobMessage.class);
    assertThat(decoded).isEqualTo(message);
  }

  @Test
  void rejectsBlankQueueUrl() {
    SqsClient client = mock(SqsClient.class);

    assertThatThrownBy(() -> new SqsJobQueueAdapter(client, " "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("queueUrl");
  }
}
