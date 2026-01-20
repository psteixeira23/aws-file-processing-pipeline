package com.patrick.awsfileprocessing.application.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record JobMessage(
    @JsonProperty("jobId") String jobId,
    @JsonProperty("inputBucket") String inputBucket,
    @JsonProperty("inputKey") String inputKey,
    @JsonProperty("outputBucket") String outputBucket,
    @JsonProperty("outputKey") String outputKey,
    @JsonProperty("createdAt") Instant createdAt,
    @JsonProperty("checksum") String checksum) {
  public JobMessage {
    requireNonBlank(jobId, "jobId");
    requireNonBlank(inputBucket, "inputBucket");
    requireNonBlank(inputKey, "inputKey");
    requireNonBlank(outputBucket, "outputBucket");
    requireNonBlank(outputKey, "outputKey");
    Objects.requireNonNull(createdAt, "createdAt must not be null");
  }

  private static void requireNonBlank(String value, String field) {
    Objects.requireNonNull(value, field + " must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
  }
}
