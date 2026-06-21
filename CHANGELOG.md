# Changelog

All notable changes to `BabelQueue.Sqs` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **OTel `traceparent` propagation (ADR-0028).** `SqsPublisher.PublishWithHeadersAsync(urn, data,
  headers, traceId)` carries an out-of-band header carrier (e.g. a W3C `traceparent` from
  `Telemetry.PublishAsync(…, headers, …)`) as **String** `MessageAttributes` **beside** the frozen
  envelope (GR-1) — merged by `SqsHeaders.Merge` so the contract `bq-*` projection always wins a key
  collision, blank keys/values are skipped, and the SQS 10-attribute cap is respected. The consume
  side surfaces inbound attributes via `public static SqsHeaders.Extract(message.MessageAttributes)`
  → `Dictionary<string,string>` to hand to `Telemetry.Wrap(handler, headers)`, so a consumer span
  becomes a true child of the producer span; with no `traceparent` it falls back to the v0.1
  `trace_id` mapping (no regression). A header-less publish is byte-identical to before.

### Changed
- Require `BabelQueue.Core 1.4.0` (the header-carrier seam version).

## [1.0.0] - 2026-06-12

### Added
- Initial release. An Amazon SQS transport on `BabelQueue.Core` + the AWS SDK for .NET v4
  (`AWSSDK.SQS`): `SqsPublisher` (canonical-envelope `SendMessage` with the §3
  `MessageAttributes` projection — `bq-job`/`bq-trace-id`/`bq-message-id`/
  `bq-schema-version`/`bq-source-lang`/`bq-created-at`; FIFO group/dedup) and
  `SqsConsumer` (long-poll receive → URN-routed `BabelHandler`s → `DeleteMessage`;
  SQS-native visibility-timeout retry; `attempts` reconciled to
  `ApproximateReceiveCount − 1`, never lowering a runtime-incremented count;
  `OnError`/`OnUnknownUrn` hooks). `net8.0`, Roslyn analyzers (latest-recommended,
  warnings-as-errors); 16 xUnit tests (incl. the cross-SDK SQS binding conformance) run
  with a Moq-mocked `IAmazonSQS` (no AWS, no network). The envelope is unchanged
  (`schema_version: 1`); SQS is purely additive.

[Unreleased]: https://github.com/BabelQueue/babelqueue-dotnet-sqs/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/BabelQueue/babelqueue-dotnet-sqs/releases/tag/v1.0.0
