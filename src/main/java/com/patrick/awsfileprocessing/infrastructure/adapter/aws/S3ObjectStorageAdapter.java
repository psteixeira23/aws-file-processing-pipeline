package com.patrick.awsfileprocessing.infrastructure.adapter.aws;

import com.patrick.awsfileprocessing.application.model.ObjectMetadata;
import com.patrick.awsfileprocessing.application.port.ObjectStoragePort;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.io.InputStream;
import java.util.Objects;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public final class S3ObjectStorageAdapter implements ObjectStoragePort {
  private final S3Client s3Client;

  public S3ObjectStorageAdapter(S3Client s3Client) {
    this.s3Client = Objects.requireNonNull(s3Client, "s3Client must not be null");
  }

  @Override
  public InputStream getObjectStream(S3Location location) {
    GetObjectRequest request =
        GetObjectRequest.builder().bucket(location.bucket()).key(location.key()).build();
    ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);
    return response;
  }

  @Override
  public void putObject(S3Location location, byte[] bytes, String contentType) {
    PutObjectRequest request =
        PutObjectRequest.builder()
            .bucket(location.bucket())
            .key(location.key())
            .contentType(contentType)
            .build();
    s3Client.putObject(request, RequestBody.fromBytes(bytes));
  }

  @Override
  public ObjectMetadata headObject(S3Location location) {
    HeadObjectRequest request =
        HeadObjectRequest.builder().bucket(location.bucket()).key(location.key()).build();
    HeadObjectResponse response = s3Client.headObject(request);
    return new ObjectMetadata(response.contentLength());
  }
}
