package com.patrick.awsfileprocessing.application.port;

import java.time.Instant;

public interface ClockPort {
  Instant now();
}
