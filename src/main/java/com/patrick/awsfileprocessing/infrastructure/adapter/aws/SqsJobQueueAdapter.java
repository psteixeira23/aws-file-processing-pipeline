package com.patrick.awsfileprocessing.infrastructure.adapter.aws;

import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.application.port.JobQueuePort;
import com.patrick.awsfileprocessing.common.JsonMapper;
import java.util.Objects;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public final class SqsJobQueueAdapter implements JobQueuePort {
  private final SqsClient sqsClient;
  private final String queueUrl;

  public SqsJobQueueAdapter(SqsClient sqsClient, String queueUrl) {
    this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient must not be null");
    this.queueUrl = requireNonBlank(queueUrl, "queueUrl");
  }

  @Override
  public void sendJob(JobMessage message) {
    String payload = JsonMapper.writeString(message);
    SendMessageRequest request =
        SendMessageRequest.builder().queueUrl(queueUrl).messageBody(payload).build();
    sqsClient.sendMessage(request);
  }

  private String requireNonBlank(String value, String field) {
    Objects.requireNonNull(value, field + " must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
