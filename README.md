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

## Build & test

```bash
dotnet test
```

`IAmazonSQS` is an interface, so the unit tests mock it with Moq — no AWS, no network.

## License

MIT
