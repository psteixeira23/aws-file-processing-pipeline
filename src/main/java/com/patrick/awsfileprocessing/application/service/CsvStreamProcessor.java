package com.patrick.awsfileprocessing.application.service;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class CsvStreamProcessor {
  private static final int BUFFER_SIZE = 8192;

  public CsvProcessingSummary process(InputStream inputStream, boolean excludeHeader)
      throws IOException {
    MessageDigest digest = sha256Digest();
    byte[] buffer = new byte[BUFFER_SIZE];
    long bytesRead = 0L;
    long newlineCount = 0L;
    boolean hasBytes = false;
    boolean lastWasNewline = false;

    int read;
    while ((read = inputStream.read(buffer)) != -1) {
      if (read > 0) {
        hasBytes = true;
        bytesRead += read;
        digest.update(buffer, 0, read);
        for (int i = 0; i < read; i++) {
          byte value = buffer[i];
          if (value == '\n') {
            newlineCount++;
          }
          lastWasNewline = value == '\n';
        }
      }
    }

    long lines = 0L;
    if (hasBytes) {
      lines = newlineCount + (lastWasNewline ? 0 : 1);
    }
    if (excludeHeader && lines > 0) {
      lines -= 1;
    }

    return new CsvProcessingSummary(lines, toHex(digest.digest()), bytesRead);
  }

  private MessageDigest sha256Digest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private String toHex(byte[] hash) {
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
