package com.patrick.awsfileprocessing.domain;

import java.util.Objects;

public final class OutputKeyConvention {
  private OutputKeyConvention() {}

  public static String outputKeyFor(JobId jobId) {
    Objects.requireNonNull(jobId, "jobId must not be null");
    return "output/" + jobId.asString() + ".json";
  }
}
