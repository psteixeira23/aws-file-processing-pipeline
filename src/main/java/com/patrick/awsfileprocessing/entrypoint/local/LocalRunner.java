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
    Path inputPath = resolveInputPath(args);
    if (inputPath == null) {
      return;
    }

    AppConfig config = AppConfig.fromEnv();
    LocalContext context = buildContext(config);
    S3Location inputLocation = seedInput(context, inputPath);
    JobId jobId = createJob(context, inputLocation);
    JobMessage jobMessage = pollJob(context);
    if (jobMessage == null) {
      return;
    }
    processJob(context, jobMessage);
    printOutput(context, jobId);
  }

  private static Path resolveInputPath(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: LocalRunner <path-to-csv>");
      return null;
    }

    Path inputPath = Path.of(args[0]);
    if (!Files.exists(inputPath)) {
      System.err.println("File not found: " + inputPath);
      return null;
    }
    return inputPath;
  }

  private static LocalContext buildContext(AppConfig config) {
    String outputBucket =
        config.outputBucket() == null ? DEFAULT_OUTPUT_BUCKET : config.outputBucket();
    return new LocalContext(
        new InMemoryObjectStorageAdapter(),
        new InMemoryJobQueueAdapter(),
        new InMemoryIdempotencyStoreAdapter(),
        outputBucket,
        config.excludeHeader());
  }

  private static S3Location seedInput(LocalContext context, Path inputPath) throws IOException {
    byte[] fileBytes = Files.readAllBytes(inputPath);
    String inputKey = inputPath.getFileName().toString();
    S3Location inputLocation = new S3Location(DEFAULT_INPUT_BUCKET, inputKey);
    context.objectStorage().putObject(inputLocation, fileBytes, "text/csv");
    return inputLocation;
  }

  private static JobId createJob(LocalContext context, S3Location inputLocation) {
    CreateJobUseCase createJobUseCase =
        new CreateJobUseCase(context.jobQueue(), new SystemClockAdapter(), context.outputBucket());
    return createJobUseCase.createJob(inputLocation);
  }

  private static JobMessage pollJob(LocalContext context) {
    JobMessage jobMessage = context.jobQueue().poll();
    if (jobMessage == null) {
      System.err.println("No job message created.");
    }
    return jobMessage;
  }

  private static void processJob(LocalContext context, JobMessage jobMessage) {
    ProcessJobUseCase processJobUseCase =
        new ProcessJobUseCase(
            context.objectStorage(),
            context.idempotencyStore(),
            new NoopMetricsAdapter(),
            new SystemClockAdapter(),
            new CsvStreamProcessor(),
            context.excludeHeader());
    processJobUseCase.process(jobMessage);
  }

  private static void printOutput(LocalContext context, JobId jobId) throws IOException {
    S3Location outputLocation =
        new S3Location(context.outputBucket(), OutputKeyConvention.outputKeyFor(jobId));
    try (InputStream outputStream = context.objectStorage().getObjectStream(outputLocation)) {
      String outputJson = new String(outputStream.readAllBytes(), StandardCharsets.UTF_8);
      System.out.println(outputJson);
    }
  }

  private record LocalContext(
      InMemoryObjectStorageAdapter objectStorage,
      InMemoryJobQueueAdapter jobQueue,
      InMemoryIdempotencyStoreAdapter idempotencyStore,
      String outputBucket,
      boolean excludeHeader) {}
}
