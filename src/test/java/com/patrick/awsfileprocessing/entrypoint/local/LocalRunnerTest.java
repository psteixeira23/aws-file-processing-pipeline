package com.patrick.awsfileprocessing.entrypoint.local;

import static org.assertj.core.api.Assertions.assertThat;

import com.patrick.awsfileprocessing.common.JsonMapper;
import com.patrick.awsfileprocessing.domain.ProcessingResult;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class LocalRunnerTest {
  @Test
  void processesCsvAndPrintsResultJson() throws Exception {
    Path tempFile = Files.createTempFile("input", ".csv");
    Files.writeString(tempFile, "a,b\n1,2\n", StandardCharsets.UTF_8);
    long fileSize = Files.size(tempFile);

    PrintStream originalOut = System.out;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
      LocalRunner.main(new String[] {tempFile.toString()});
    } finally {
      System.setOut(originalOut);
      Files.deleteIfExists(tempFile);
    }

    String jsonLine = lastNonBlankLine(output.toString(StandardCharsets.UTF_8));
    ProcessingResult result = JsonMapper.read(jsonLine, ProcessingResult.class);

    assertThat(result.inputBucket()).isEqualTo("local-input");
    assertThat(result.outputBucket()).isEqualTo("local-output");
    assertThat(result.outputKey()).isEqualTo("output/" + result.jobId() + ".json");
    assertThat(result.totalLines()).isEqualTo(2);
    assertThat(result.fileSizeBytes()).isEqualTo(fileSize);
  }

  private static String lastNonBlankLine(String output) {
    String[] lines = output.split("\\R");
    for (int index = lines.length - 1; index >= 0; index--) {
      String line = lines[index].trim();
      if (!line.isEmpty()) {
        return line;
      }
    }
    return output.trim();
  }
}
