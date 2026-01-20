package com.patrick.awsfileprocessing.infrastructure.adapter;

import com.patrick.awsfileprocessing.application.port.ClockPort;
import java.time.Instant;

public final class SystemClockAdapter implements ClockPort {
  @Override
  public Instant now() {
    return Instant.now();
  }
}
