# BabelQueue.Sqs

Amazon SQS transport for [BabelQueue](https://babelqueue.com) — "Polyglot Queues,
Simplified." Built on the AWS SDK for .NET and the framework-agnostic
[`BabelQueue.Core`](https://www.nuget.org/packages/BabelQueue.Core).

A canonical-envelope **publisher** and a URN-routed **consumer**, so an SQS-based .NET
service speaks the same wire contract (envelope shape, URN identity, trace propagation)
as the PHP/Laravel, Python, Go, Node and Java SDKs. Implements
[§3 of the broker-bindings contract](https://babelqueue.com).

## Install

```bash
dotnet add package BabelQueue.Sqs
```

It pulls `BabelQueue.Core` and `AWSSDK.SQS` transitively.

## Use

```csharp
using Amazon.SQS;
using BabelQueue.Sqs;

IAmazonSQS sqs = new AmazonSQSClient(); // your AWS config / credentials chain
var url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

// produce
var id = await new SqsPublisher(sqs, url)
    .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 1042 });

// consume
var handlers = new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = async (env, message, ct) =>
    {
        // env.Data, env.TraceId, env.Attempts ...
    },
};
var consumer = new SqsConsumer(sqs, url, handlers, new SqsConsumerOptions
{
    OnError = (err, env, msg) => Console.Error.WriteLine(err),
});
await consumer.RunAsync(cancellationToken); // long-polls until cancelled
```

FIFO: `new SqsPublisher(sqs, url, fifo: true)` (the URL must end in `.fifo`). For
LocalStack/ElasticMQ, point the `AmazonSQSClient`'s `ServiceURL` there.

## Contract mapping (§3)

| Envelope | SQS |
| :--- | :--- |
| body | `MessageBody` (byte-identical across SDKs) |
| `job` (URN) | `MessageAttributes.bq-job` |
| `trace_id` | `MessageAttributes.bq-trace-id` |
| `meta.id` | `MessageAttributes.bq-message-id` |
| `meta.schema_version` | `MessageAttributes.bq-schema-version` (Number) |
| `meta.lang` | `MessageAttributes.bq-source-lang` |
| `meta.created_at` | `MessageAttributes.bq-created-at` (Number, ms) |
| `attempts` | reconciled to `ApproximateReceiveCount − 1` on receive |
| reserve / ack | visibility timeout → `DeleteMessage` |

Retry is **SQS-native**: a throwing handler leaves the message undeleted, so SQS
redelivers it after the visibility timeout (at-least-once). The poll loop never stops on
a bad message — observe via `OnError` / `OnUnknownUrn`. The envelope is unchanged
(`schema_version` stays `1`); SQS is purely additive.

## OpenTelemetry `traceparent` propagation (ADR-0028)

For true cross-hop **span** parent-child linkage, the active producer span's W3C `traceparent`
rides as a String `MessageAttribute` **beside** the frozen envelope (never inside it). Produce with
the header-aware overload; the carrier is filled by `BabelQueue.Core`'s `Telemetry.PublishAsync`:

```csharp
using BabelQueue.Tracing;

var headers = new Dictionary<string, string>();
await Telemetry.PublishAsync("urn:babel:orders:created", data, headers,
    env => new SqsPublisher(sqs, url).PublishWithHeadersAsync("urn:babel:orders:created", data, headers));
```

On the consume side, surface the inbound attributes and start the consumer span as a child:

```csharp
["urn:babel:orders:created"] = Telemetry.Wrap(
    async env => { /* ... */ },
    SqsHeaders.Extract(message.MessageAttributes)) // call inside the handler with the raw Message
```

`SqsHeaders.Merge` never clobbers the contract `bq-*` attributes and respects the SQS 10-attribute
cap; with no `traceparent` the consumer falls back to the v0.1 `trace_id` mapping. A header-less
publish is byte-identical to `PublishAsync`. Requires `BabelQueue.Core 1.4.0`.

## Build & test

```bash
dotnet test
```

`IAmazonSQS` is an interface, so the unit tests mock it with Moq — no AWS, no network.

## License

MIT
