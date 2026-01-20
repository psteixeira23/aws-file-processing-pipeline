package com.patrick.awsfileprocessing.infrastructure.adapter;

import com.patrick.awsfileprocessing.application.port.MetricsPort;
import java.util.Map;

public final class NoopMetricsAdapter implements MetricsPort {
  @Override
  public void incrementCounter(String name, Map<String, String> tags) {}

  @Override
  public void recordTiming(String name, long milliseconds, Map<String, String> tags) {}
}
