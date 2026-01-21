package com.patrick.awsfileprocessing.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class AppConfigTest {
  @Test
  void fromEnvDefaultsToInMemoryIdempotency() {
    Map<String, String> env =
        Map.of(
            "INPUT_BUCKET", "input",
            "OUTPUT_BUCKET", "output",
            "JOB_QUEUE_URL", "queue-url",
            "AWS_REGION", "us-east-1");

    AppConfig config = AppConfig.fromEnv(env);

    assertThat(config.idempotencyMode()).isEqualTo(AppConfig.IdempotencyMode.IN_MEMORY);
    assertThat(config.excludeHeader()).isFalse();
  }

  @Test
  void requireDynamoTableValidatesWhenDynamoIsEnabled() {
    Map<String, String> env =
        Map.of(
            "INPUT_BUCKET", "input",
            "OUTPUT_BUCKET", "output",
            "JOB_QUEUE_URL", "queue-url",
            "AWS_REGION", "us-east-1",
            "IDP_MODE", "DYNAMODB");

    AppConfig config = AppConfig.fromEnv(env);

    assertThatThrownBy(config::requireDynamoTable)
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("DDB_TABLE_NAME");
  }

  @Test
  void requireOutputBucketRejectsBlankValue() {
    AppConfig config =
        new AppConfig(
            "input",
            " ",
            "queue-url",
            "us-east-1",
            AppConfig.IdempotencyMode.IN_MEMORY,
            null,
            false);

    assertThatThrownBy(config::requireOutputBucket)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("OUTPUT_BUCKET");
  }
}
