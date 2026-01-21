package com.patrick.awsfileprocessing.entrypoint.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.patrick.awsfileprocessing.application.port.ClockPort;
import com.patrick.awsfileprocessing.application.port.JobQueuePort;
import com.patrick.awsfileprocessing.application.usecase.CreateJobUseCase;
import com.patrick.awsfileprocessing.common.AppConfig;
import com.patrick.awsfileprocessing.domain.S3Location;
import com.patrick.awsfileprocessing.infrastructure.adapter.SystemClockAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.aws.SqsJobQueueAdapter;
import java.util.Objects;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

public final class S3EventPublisherLambda implements RequestHandler<S3Event, Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3EventPublisherLambda.class);

  private final Consumer<S3Location> jobCreator;

  public S3EventPublisherLambda() {
    AppConfig config =
        AppConfig.fromEnv().requireOutputBucket().requireJobQueueUrl().requireAwsRegion();

    SqsClient sqsClient = SqsClient.builder().region(Region.of(config.awsRegion())).build();

    JobQueuePort jobQueuePort = new SqsJobQueueAdapter(sqsClient, config.jobQueueUrl());
    ClockPort clockPort = new SystemClockAdapter();
    CreateJobUseCase createJobUseCase =
        new CreateJobUseCase(jobQueuePort, clockPort, config.outputBucket());
    this.jobCreator = createJobUseCase::createJob;
  }

  S3EventPublisherLambda(Consumer<S3Location> jobCreator) {
    this.jobCreator = Objects.requireNonNull(jobCreator, "jobCreator must not be null");
  }

  @Override
  public Void handleRequest(S3Event event, Context context) {
    if (event == null || event.getRecords() == null) {
      LOGGER.warn("s3_event_empty");
      return null;
    }

    event
        .getRecords()
        .forEach(
            record -> {
              String bucket = record.getS3().getBucket().getName();
              String key = record.getS3().getObject().getKey();
              jobCreator.accept(new S3Location(bucket, key));
            });

    return null;
  }
}
