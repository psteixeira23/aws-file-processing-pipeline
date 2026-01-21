package com.patrick.awsfileprocessing.infrastructure.adapter.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.patrick.awsfileprocessing.application.model.ObjectMetadata;
import com.patrick.awsfileprocessing.domain.S3Location;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3ObjectStorageAdapterTest {
  @Test
  void getObjectStreamReturnsObjectBytes() throws Exception {
    S3Client client = mock(S3Client.class);
    byte[] payload = "data".getBytes(StandardCharsets.UTF_8);
    ResponseInputStream<GetObjectResponse> response =
        new ResponseInputStream<>(
            GetObjectResponse.builder().contentLength((long) payload.length).build(),
            new ByteArrayInputStream(payload));
    when(client.getObject(any(GetObjectRequest.class))).thenReturn(response);

    S3ObjectStorageAdapter adapter = new S3ObjectStorageAdapter(client);
    S3Location location = new S3Location("bucket", "key.csv");

    try (InputStream inputStream = adapter.getObjectStream(location)) {
      assertThat(inputStream.readAllBytes()).isEqualTo(payload);
    }

    ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
    verify(client).getObject(captor.capture());
    assertThat(captor.getValue().bucket()).isEqualTo("bucket");
    assertThat(captor.getValue().key()).isEqualTo("key.csv");
  }

  @Test
  void putObjectSendsPayloadWithContentType() throws Exception {
    S3Client client = mock(S3Client.class);
    S3ObjectStorageAdapter adapter = new S3ObjectStorageAdapter(client);
    S3Location location = new S3Location("bucket", "key.json");
    byte[] payload = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);

    adapter.putObject(location, payload, "application/json");

    ArgumentCaptor<PutObjectRequest> requestCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
    verify(client).putObject(requestCaptor.capture(), bodyCaptor.capture());

    PutObjectRequest request = requestCaptor.getValue();
    assertThat(request.bucket()).isEqualTo("bucket");
    assertThat(request.key()).isEqualTo("key.json");
    assertThat(request.contentType()).isEqualTo("application/json");

    RequestBody body = bodyCaptor.getValue();
    assertThat(body.contentLength()).isEqualTo(payload.length);
    try (InputStream bodyStream = body.contentStreamProvider().newStream()) {
      assertThat(bodyStream.readAllBytes()).isEqualTo(payload);
    }
  }

  @Test
  void headObjectReturnsContentLength() {
    S3Client client = mock(S3Client.class);
    when(client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(HeadObjectResponse.builder().contentLength(42L).build());

    S3ObjectStorageAdapter adapter = new S3ObjectStorageAdapter(client);

    ObjectMetadata metadata = adapter.headObject(new S3Location("bucket", "key.csv"));

    assertThat(metadata.sizeBytes()).isEqualTo(42L);
    ArgumentCaptor<HeadObjectRequest> captor = ArgumentCaptor.forClass(HeadObjectRequest.class);
    verify(client).headObject(captor.capture());
    assertThat(captor.getValue().bucket()).isEqualTo("bucket");
    assertThat(captor.getValue().key()).isEqualTo("key.csv");
  }
}
