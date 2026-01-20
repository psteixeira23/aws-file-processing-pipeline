package com.patrick.awsfileprocessing.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.io.IOException;
import java.io.InputStream;

public final class JsonMapper {
  private static final ObjectMapper MAPPER = buildMapper();

  private JsonMapper() {}

  public static byte[] writeBytes(Object value) {
    try {
      return MAPPER.writeValueAsBytes(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize JSON", e);
    }
  }

  public static String writeString(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize JSON", e);
    }
  }

  public static <T> T read(String json, Class<T> type) {
    try {
      return MAPPER.readValue(json, type);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse JSON", e);
    }
  }

  public static <T> T read(InputStream inputStream, Class<T> type) {
    try {
      return MAPPER.readValue(inputStream, type);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse JSON", e);
    }
  }

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  private static ObjectMapper buildMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new ParameterNamesModule());
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    return mapper;
  }
}
