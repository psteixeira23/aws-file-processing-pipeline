package com.patrick.awsfileprocessing.entrypoint.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.application.port.ClockPort;
import com.patrick.awsfileprocessing.application.port.IdempotencyStorePort;
import com.patrick.awsfileprocessing.application.port.MetricsPort;
import com.patrick.awsfileprocessing.application.port.ObjectStoragePort;
import com.patrick.awsfileprocessing.application.service.CsvStreamProcessor;
import com.patrick.awsfileprocessing.application.usecase.ProcessJobUseCase;
import com.patrick.awsfileprocessing.common.AppConfig;
import com.patrick.awsfileprocessing.common.JsonMapper;
import com.patrick.awsfileprocessing.infrastructure.adapter.NoopMetricsAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.SystemClockAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.aws.DynamoDbIdempotencyStoreAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.aws.S3ObjectStorageAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.inmemory.InMemoryIdempotencyStoreAdapter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

public final class SqsWorkerLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqsWorkerLambda.class);

  private final Consumer<JobMessage> jobProcessor;

  public SqsWorkerLambda() {
    AppConfig config = AppConfig.fromEnv().requireAwsRegion().requireDynamoTable();

    Region region = Region.of(config.awsRegion());
    S3Client s3Client = S3Client.builder().region(region).build();

    ObjectStoragePort objectStoragePort = new S3ObjectStorageAdapter(s3Client);
    IdempotencyStorePort idempotencyStorePort = buildIdempotencyStore(config, region);
    MetricsPort metricsPort = new NoopMetricsAdapter();
    ClockPort clockPort = new SystemClockAdapter();

    ProcessJobUseCase processJobUseCase =
        new ProcessJobUseCase(
            objectStoragePort,
            idempotencyStorePort,
            metricsPort,
            clockPort,
            new CsvStreamProcessor(),
            config.excludeHeader());
    this.jobProcessor = processJobUseCase::process;
  }

  SqsWorkerLambda(Consumer<JobMessage> jobProcessor) {
    this.jobProcessor = Objects.requireNonNull(jobProcessor, "jobProcessor must not be null");
  }

  @Override
  public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
    List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();
    if (event == null || event.getRecords() == null) {
      LOGGER.warn("sqs_event_empty");
      return new SQSBatchResponse(failures);
    }

    for (SQSEvent.SQSMessage record : event.getRecords()) {
      try {
        JobMessage message = JsonMapper.read(record.getBody(), JobMessage.class);
        jobProcessor.accept(message);
      } catch (Exception e) {
        LOGGER.error(
            "sqs_message_failed messageId={} errorType={} message={}",
            record.getMessageId(),
            e.getClass().getSimpleName(),
            e.getMessage(),
            e);
        failures.add(new SQSBatchResponse.BatchItemFailure(record.getMessageId()));
      }
    }

    return new SQSBatchResponse(failures);
  }

  private IdempotencyStorePort buildIdempotencyStore(AppConfig config, Region region) {
    if (config.idempotencyMode() == AppConfig.IdempotencyMode.DYNAMODB) {
      DynamoDbClient dynamoDbClient = DynamoDbClient.builder().region(region).build();
      return new DynamoDbIdempotencyStoreAdapter(dynamoDbClient, config.ddbTableName());
    }
    return new InMemoryIdempotencyStoreAdapter();
  }
}
