package com.patrick.awsfileprocessing.entrypoint.local;

import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.application.service.CsvStreamProcessor;
import com.patrick.awsfileprocessing.application.usecase.CreateJobUseCase;
import com.patrick.awsfileprocessing.application.usecase.ProcessJobUseCase;
import com.patrick.awsfileprocessing.common.AppConfig;
import com.patrick.awsfileprocessing.domain.JobId;
import com.patrick.awsfileprocessing.domain.OutputKeyConvention;
import com.patrick.awsfileprocessing.domain.S3Location;
import com.patrick.awsfileprocessing.infrastructure.adapter.NoopMetricsAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.SystemClockAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.inmemory.InMemoryIdempotencyStoreAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.inmemory.InMemoryJobQueueAdapter;
import com.patrick.awsfileprocessing.infrastructure.adapter.inmemory.InMemoryObjectStorageAdapter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class LocalRunner {
  private static final String DEFAULT_INPUT_BUCKET = "local-input";
  private static final String DEFAULT_OUTPUT_BUCKET = "local-output";

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: LocalRunner <path-to-csv>");
      return;
    }

    Path inputPath = Path.of(args[0]);
    if (!Files.exists(inputPath)) {
      System.err.println("File not found: " + inputPath);
      return;
    }

    AppConfig config = AppConfig.fromEnv();
    String outputBucket =
        config.outputBucket() == null ? DEFAULT_OUTPUT_BUCKET : config.outputBucket();

    InMemoryObjectStorageAdapter objectStorage = new InMemoryObjectStorageAdapter();
    InMemoryJobQueueAdapter jobQueue = new InMemoryJobQueueAdapter();
    InMemoryIdempotencyStoreAdapter idempotencyStore = new InMemoryIdempotencyStoreAdapter();

    byte[] fileBytes = Files.readAllBytes(inputPath);
    String inputKey = inputPath.getFileName().toString();
    S3Location inputLocation = new S3Location(DEFAULT_INPUT_BUCKET, inputKey);
    objectStorage.putObject(inputLocation, fileBytes, "text/csv");

    CreateJobUseCase createJobUseCase =
        new CreateJobUseCase(jobQueue, new SystemClockAdapter(), outputBucket);
    JobId jobId = createJobUseCase.createJob(inputLocation);

    JobMessage jobMessage = jobQueue.poll();
    if (jobMessage == null) {
      System.err.println("No job message created.");
      return;
    }

    ProcessJobUseCase processJobUseCase =
        new ProcessJobUseCase(
            objectStorage,
            idempotencyStore,
            new NoopMetricsAdapter(),
            new SystemClockAdapter(),
            new CsvStreamProcessor(),
            config.excludeHeader());

    processJobUseCase.process(jobMessage);

    S3Location outputLocation =
        new S3Location(outputBucket, OutputKeyConvention.outputKeyFor(jobId));
    try (InputStream outputStream = objectStorage.getObjectStream(outputLocation)) {
      String outputJson = new String(outputStream.readAllBytes(), StandardCharsets.UTF_8);
      System.out.println(outputJson);
    }
  }
}
