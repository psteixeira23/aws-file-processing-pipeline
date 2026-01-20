package com.patrick.awsfileprocessing.application.usecase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.application.port.ClockPort;
import com.patrick.awsfileprocessing.application.port.JobQueuePort;
import com.patrick.awsfileprocessing.domain.JobId;
import com.patrick.awsfileprocessing.domain.OutputKeyConvention;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class CreateJobUseCaseTest {
  @Test
  void createJobPublishesMessage() {
    CapturingJobQueue queue = new CapturingJobQueue();
    FixedClock clock = new FixedClock(Instant.parse("2024-01-01T10:00:00Z"));
    CreateJobUseCase useCase = new CreateJobUseCase(queue, clock, "output-bucket");

    JobId jobId = useCase.createJob(new S3Location("input-bucket", "input.csv"));

    JobMessage message = queue.message;
    assertThat(message).isNotNull();
    assertThat(message.jobId()).isEqualTo(jobId.asString());
    assertThat(message.inputBucket()).isEqualTo("input-bucket");
    assertThat(message.inputKey()).isEqualTo("input.csv");
    assertThat(message.outputBucket()).isEqualTo("output-bucket");
    assertThat(message.outputKey()).isEqualTo(OutputKeyConvention.outputKeyFor(jobId));
    assertThat(message.createdAt()).isEqualTo(clock.now());
  }

  @Test
  void rejectsNonCsvFile() {
    CapturingJobQueue queue = new CapturingJobQueue();
    FixedClock clock = new FixedClock(Instant.parse("2024-01-01T10:00:00Z"));
    CreateJobUseCase useCase = new CreateJobUseCase(queue, clock, "output-bucket");

    assertThatThrownBy(() -> useCase.createJob(new S3Location("input-bucket", "input.txt")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported file type");
  }

  private static final class CapturingJobQueue implements JobQueuePort {
    private JobMessage message;

    @Override
    public void sendJob(JobMessage message) {
      this.message = message;
    }
  }

  private static final class FixedClock implements ClockPort {
    private final Instant now;

    private FixedClock(Instant now) {
      this.now = now;
    }

    @Override
    public Instant now() {
      return now;
    }
  }
}
