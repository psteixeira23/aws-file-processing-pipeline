package com.patrick.awsfileprocessing.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class JsonMapperTest {
  @Test
  void readsFromInputStream() {
    String payload = "{\"name\":\"test\"}";
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));

    Sample value = JsonMapper.read(inputStream, Sample.class);

    assertThat(value.name).isEqualTo("test");
  }

  @Test
  void writeBytesProducesJson() {
    Sample sample = new Sample("ok");

    byte[] bytes = JsonMapper.writeBytes(sample);

    assertThat(new String(bytes, StandardCharsets.UTF_8)).contains("\"name\":\"ok\"");
  }

  @Test
  void readInvalidJsonThrows() {
    assertThatThrownBy(() -> JsonMapper.read("not-json", Sample.class))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to parse JSON");
  }

  private record Sample(String name) {}
}
