# Changelog

All notable changes to `BabelQueue.Sqs` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Fixed
- `SqsConsumer` now checks the `bq-schema-version` message attribute **before** decoding the body (§3.7). A value other than exactly `"1"` (ordinal, untrimmed) is rejected without decoding: `OnError` is invoked and the message is neither deleted nor released, so the queue's redrive policy takes over. Absent or blank (empty / ASCII whitespace) falls through to the normal decode. Mirrors the Pulsar gate; no public API change.

## [1.2.1] - 2026-10-03

### Changed
- Raise the `AWSSDK.SQS` floor from 4.0.3.3 to 4.0.100.15. No API or behaviour change.

## [1.2.0] - 2026-10-03

MINOR: new public options and a changed failure-path behaviour (see below); no existing
signature changed.

### Changed
- **Release uses the contract path (broker-bindings.md §3.5).** A throwing handler now always
  releases the message with `ChangeMessageVisibility(ReceiptHandle, VisibilityTimeout = backoff)`
  instead of silently waiting out the queue's visibility timeout; it is never deleted, so
  `ApproximateReceiveCount` stays the attempt counter. New `SqsConsumerOptions.RetryBackoffSeconds`
  (default `0` = redeliver now; clamped to 0–43200).
- **⚠ Poison-loop risk.** With the default `0`, a message whose handler always throws is
  redelivered immediately and indefinitely (previously it waited out the visibility timeout).
  **Configure a native redrive policy** on the source queue (`RedrivePolicy` with a
  `maxReceiveCount`, e.g. 5, targeting `<queue>.dlq`) so SQS moves it aside; set
  `RetryBackoffSeconds` for a delay between attempts.
- Require `BabelQueue.Core 1.8.0` — unknown envelope keys now survive every re-emit (dead-letter
  included) and forbidden keys are dropped with a warning.

### Added
- **Unknown-URN strategies.** New `SqsConsumerOptions.UnknownUrnStrategy` (`fail` default — unchanged
  behaviour — / `delete` / `release` / `dead_letter`), `UnknownUrnReleaseDelaySeconds` (default `0`,
  applied via `ChangeMessageVisibility`; the same poison-loop caveat applies) and `DeadLetterQueueUrl`
  (`dead_letter` sends the envelope with a `dead_letter` block to `<queue>.dlq`, then deletes;
  degrades to delete without a DLQ URL). A FIFO DLQ (`<queue>.dlq.fifo`) is sent with
  `MessageGroupId = meta.queue` and `MessageDeduplicationId = meta.id`. An `OnUnknownUrn` hook
  still takes precedence. An unrecognised strategy string is rejected by the `SqsConsumer`
  constructor (`ArgumentException`).

### Fixed
- **The poll loop survives failure-path broker errors.** A failing release, dead-letter send or
  unknown-URN delete (e.g. `ReceiptHandleIsInvalid` after the visibility window lapsed,
  throttling, a network fault) is reported via `OnError` and the message is left to visibility
  expiry; the rest of the batch keeps processing. On shutdown (cancelled token) a throwing
  handler's release is skipped.
- **A failing delete after a successful handler is no longer treated as a handler failure.** It
  is reported via `OnError` as a `BabelQueueException` wrapping the broker error and the message
  is **not** released (it returns on visibility expiry), so a throttled `DeleteMessage` no longer
  triggers an immediate redelivery of an already-processed message.
- `UnknownUrnStrategy` is snapshotted at construction; changing the options object afterwards
  no longer bypasses validation.

## [1.1.0] - 2026-06-21

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

[Unreleased]: https://github.com/BabelQueue/babelqueue-dotnet-sqs/compare/v1.2.1...HEAD
[1.2.1]: https://github.com/BabelQueue/babelqueue-dotnet-sqs/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/BabelQueue/babelqueue-dotnet-sqs/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/BabelQueue/babelqueue-dotnet-sqs/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/BabelQueue/babelqueue-dotnet-sqs/releases/tag/v1.0.0
