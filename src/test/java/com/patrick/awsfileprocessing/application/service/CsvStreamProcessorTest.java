package com.patrick.awsfileprocessing.application.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.junit.jupiter.api.Test;

class CsvStreamProcessorTest {
  @Test
  void countsLinesAndCalculatesChecksum() throws IOException, NoSuchAlgorithmException {
    String csv = "name,age\nana,30\nleo,41\n";
    CsvStreamProcessor processor = new CsvStreamProcessor();

    CsvProcessingSummary summary =
        processor.process(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), false);

    assertThat(summary.totalLines()).isEqualTo(3);
    assertThat(summary.checksum()).isEqualTo(sha256Hex(csv));
    assertThat(summary.bytesRead()).isEqualTo(csv.getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  void excludesHeaderWhenConfigured() throws IOException, NoSuchAlgorithmException {
    String csv = "h1,h2\n1,2\n3,4";
    CsvStreamProcessor processor = new CsvStreamProcessor();

    CsvProcessingSummary summary =
        processor.process(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), true);

    assertThat(summary.totalLines()).isEqualTo(2);
    assertThat(summary.checksum()).isEqualTo(sha256Hex(csv));
  }

  private String sha256Hex(String payload) throws NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] hash = digest.digest(payload.getBytes(StandardCharsets.UTF_8));
    StringBuilder builder = new StringBuilder(hash.length * 2);
    for (byte value : hash) {
      String hex = Integer.toHexString(0xff & value);
      if (hex.length() == 1) {
        builder.append('0');
      }
      builder.append(hex);
    }
    return builder.toString();
  }
}
