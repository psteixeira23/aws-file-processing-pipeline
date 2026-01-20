package com.patrick.awsfileprocessing.application.port;

import com.patrick.awsfileprocessing.application.model.JobMessage;

public interface JobQueuePort {
  void sendJob(JobMessage message);
}
