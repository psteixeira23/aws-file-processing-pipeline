# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-20
### Added
- Initial MVP for the AWS file processing pipeline.
- Clean Architecture layers with ports and adapters.
- CSV streaming processing with SHA-256 checksum.
- Idempotency support and retry-safe worker flow.
- AWS Lambda entrypoints for S3 publisher and SQS worker.
- Unit tests, CI quality gates, and documentation.
