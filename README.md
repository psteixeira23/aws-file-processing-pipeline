# aws-file-processing-pipeline

Clean architecture foundation for a Java 21 application that will process files using AWS services.

## Structure

- `domain`: Pure domain models and value objects.
- `application`: Use cases, application services, and ports (interfaces).
- `infrastructure`: Adapters for external systems (AWS S3, SQS, logging, metrics).
- `entrypoint`: Application entry points (e.g., AWS Lambda handlers).
- `common`: Cross-cutting concerns such as configuration, logging, and utilities.

## Build

This project uses Maven and targets Java 21.
