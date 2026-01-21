package com.patrick.awsfileprocessing.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class S3LocationTest {
  @Test
  void acceptsValidBucketAndKey() {
    S3Location location = new S3Location("bucket", "key.csv");

    assertThat(location.bucket()).isEqualTo("bucket");
    assertThat(location.key()).isEqualTo("key.csv");
  }

  @Test
  void rejectsBlankBucket() {
    assertThatThrownBy(() -> new S3Location(" ", "key"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bucket");
  }

  @Test
  void rejectsNullKey() {
    assertThatThrownBy(() -> new S3Location("bucket", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("key");
  }
}
