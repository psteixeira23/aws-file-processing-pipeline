package com.patrick.awsfileprocessing.application.port;

import java.util.Map;

public interface MetricsPort {
  void incrementCounter(String name, Map<String, String> tags);

  void recordTiming(String name, long milliseconds, Map<String, String> tags);
}
