package com.patrick.awsfileprocessing.application.port;

import com.patrick.awsfileprocessing.application.model.ObjectMetadata;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.io.InputStream;

public interface ObjectStoragePort {
  InputStream getObjectStream(S3Location location);

  void putObject(S3Location location, byte[] bytes, String contentType);

  ObjectMetadata headObject(S3Location location);
}
