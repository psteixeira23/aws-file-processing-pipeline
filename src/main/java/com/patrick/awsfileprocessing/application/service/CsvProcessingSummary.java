package com.patrick.awsfileprocessing.application.service;

public record CsvProcessingSummary(long totalLines, String checksum, long bytesRead) {}
