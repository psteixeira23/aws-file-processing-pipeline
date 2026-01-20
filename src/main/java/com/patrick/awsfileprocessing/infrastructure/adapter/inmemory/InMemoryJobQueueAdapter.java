package com.patrick.awsfileprocessing.infrastructure.adapter.inmemory;

import com.patrick.awsfileprocessing.application.model.JobMessage;
import com.patrick.awsfileprocessing.application.port.JobQueuePort;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class InMemoryJobQueueAdapter implements JobQueuePort {
  private final Queue<JobMessage> queue = new ConcurrentLinkedQueue<>();

  @Override
  public void sendJob(JobMessage message) {
    Objects.requireNonNull(message, "message must not be null");
    queue.add(message);
  }

  public JobMessage poll() {
    return queue.poll();
  }

  public int size() {
    return queue.size();
  }
}
