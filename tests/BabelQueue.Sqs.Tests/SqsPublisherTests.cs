using System.Diagnostics;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using BabelQueue;
using BabelQueue.Sqs;
using BabelQueue.Tracing;
using Moq;
using Xunit;

namespace BabelQueue.Sqs.Tests;

public sealed class SqsPublisherTests
{
    private const string Url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

    private static (Mock<IAmazonSQS> Mock, Func<SendMessageRequest?> Captured) MockSqs()
    {
        SendMessageRequest? captured = null;
        var mock = new Mock<IAmazonSQS>();
        mock.Setup(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<SendMessageRequest, CancellationToken>((req, _) => captured = req)
            .ReturnsAsync(new SendMessageResponse { MessageId = "sqs-1" });
        return (mock, () => captured);
    }

    [Fact]
    public async Task PublishProjectsContractAttributes()
    {
        var (mock, captured) = MockSqs();
        var data = new Dictionary<string, object?> { ["order_id"] = 1042 };

        var id = await new SqsPublisher(mock.Object, Url).PublishAsync("urn:babel:orders:created", data);

        var sent = captured()!;
        Assert.Equal(Url, sent.QueueUrl);

        using var body = JsonDocument.Parse(sent.MessageBody);
        Assert.Equal("urn:babel:orders:created", body.RootElement.GetProperty("job").GetString());
        Assert.Equal(id, body.RootElement.GetProperty("meta").GetProperty("id").GetString());

        var attrs = sent.MessageAttributes;
        Assert.Equal("urn:babel:orders:created", attrs["bq-job"].StringValue);
        Assert.Equal("String", attrs["bq-job"].DataType);
        Assert.Equal("1", attrs["bq-schema-version"].StringValue);
        Assert.Equal("Number", attrs["bq-schema-version"].DataType);
        Assert.Equal("dotnet", attrs["bq-source-lang"].StringValue);
        Assert.Equal(id, attrs["bq-message-id"].StringValue);
        Assert.False(string.IsNullOrEmpty(attrs["bq-trace-id"].StringValue));
        Assert.False(string.IsNullOrEmpty(attrs["bq-created-at"].StringValue));
        Assert.Null(sent.MessageGroupId);
    }

    [Fact]
    public async Task FifoSetsGroupAndDedup()
    {
        var (mock, captured) = MockSqs();
        var fifoUrl = Url + ".fifo";

        var id = await new SqsPublisher(mock.Object, fifoUrl, fifo: true)
            .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 });

        var sent = captured()!;
        Assert.Equal("orders.fifo", sent.MessageGroupId);
        Assert.Equal(id, sent.MessageDeduplicationId);
    }

    [Fact]
    public async Task ContentDedupOmitsDedupId()
    {
        var (mock, captured) = MockSqs();

        await new SqsPublisher(mock.Object, Url + ".fifo", fifo: true, messageGroupId: "grp", contentDedup: true)
            .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 });

        var sent = captured()!;
        Assert.Equal("grp", sent.MessageGroupId);
        Assert.Null(sent.MessageDeduplicationId);
    }

    [Fact]
    public async Task ClientErrorPropagates()
    {
        var mock = new Mock<IAmazonSQS>();
        mock.Setup(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("aws down"));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new SqsPublisher(mock.Object, Url).PublishAsync("urn:x:y"));
        Assert.Equal("aws down", ex.Message);
    }

    [Fact]
    public void NullArgumentsAreRejected()
    {
        Assert.Throws<ArgumentNullException>(() => new SqsPublisher(null!, Url));
        Assert.Throws<ArgumentException>(() => new SqsPublisher(new Mock<IAmazonSQS>().Object, ""));
    }

    // O6: the envelope Telemetry.PublishAsync builds is the one that is sent, so the producer span's
    // messaging.message.id is the meta.id on the wire (and the traceparent rides beside the envelope).
    [Fact]
    public async Task TelemetryPublishEnvelopeRecordsTheWireMessageId()
    {
        var (mock, captured) = MockSqs();
        var publisher = new SqsPublisher(mock.Object, Url);
        const string envelopeUrn = "urn:babel:orders:envelope-id";
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Telemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                if (activity.Kind == ActivityKind.Producer && Equals(activity.GetTagItem("babelqueue.urn"), envelopeUrn))
                {
                    lock (activities)
                    {
                        activities.Add(activity);
                    }
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        var headers = new Dictionary<string, string>();
        var id = await Telemetry.PublishAsync(
            envelopeUrn,
            new Dictionary<string, object?> { ["order_id"] = 7 },
            headers,
            env => publisher.PublishEnvelopeAsync(env, headers),
            "orders");

        var producerSpan = Assert.Single(activities);
        var request = captured()!;
        var body = request.MessageBody;
        var traceparent = request.MessageAttributes.TryGetValue("traceparent", out var attr) ? attr.StringValue : null;
        var wire = EnvelopeCodec.Decode(body);

        Assert.Equal(id, wire.Meta!.Id);
        Assert.Equal(wire.Meta.Id, producerSpan.GetTagItem("messaging.message.id"));
        Assert.Equal(Telemetry.UuidOf(producerSpan.TraceId), wire.TraceId);
        Assert.Equal(wire.TraceId, producerSpan.GetTagItem("messaging.message.conversation_id"));
        Assert.Equal(EnvelopeCodec.SchemaVersion, wire.Meta.SchemaVersion);
        Assert.Equal(headers["traceparent"], traceparent);
        Assert.Contains(producerSpan.TraceId.ToHexString(), traceparent, StringComparison.Ordinal);
        Assert.DoesNotContain("traceparent", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PublishEnvelopeSendsTheGivenEnvelopeUnchanged()
    {
        var (mock, captured) = MockSqs();
        var publisher = new SqsPublisher(mock.Object, Url);
        var made = EnvelopeCodec.Make(
            "urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-fixed");
        var envelope = made with
        {
            Extras = new Dictionary<string, JsonElement> { ["x_tenant"] = JsonSerializer.SerializeToElement("t1") },
            Meta = made.Meta! with
            {
                Extras = new Dictionary<string, JsonElement> { ["x_region"] = JsonSerializer.SerializeToElement("eu") },
            },
        };

        var id = await publisher.PublishEnvelopeAsync(envelope);

        var request = captured()!;
        var body = request.MessageBody;
        var traceparent = request.MessageAttributes.TryGetValue("traceparent", out var attr) ? attr.StringValue : null;
        Assert.Equal(envelope.Meta!.Id, id);
        Assert.Equal(EnvelopeCodec.Encode(envelope), body);
        var wire = EnvelopeCodec.Decode(body);
        Assert.Equal("trace-fixed", wire.TraceId);
        Assert.Equal(EnvelopeCodec.SchemaVersion, wire.Meta!.SchemaVersion);
        Assert.Equal(envelope.Meta.Id, wire.Meta.Id);
        Assert.Equal("t1", wire.Extras!["x_tenant"].GetString());
        Assert.Equal("eu", wire.Meta.Extras!["x_region"].GetString());
        Assert.Null(traceparent);
    }

    // Documents the PublishEnvelopeAsync contract: meta.queue is whatever the envelope carries. Telemetry stamps
    // its queue argument (default "default"), not this publisher's target, so callers must pass the queue.
    [Fact]
    public async Task TelemetryPublishWithoutAQueueArgumentStampsDefaultMetaQueue()
    {
        var (mock, captured) = MockSqs();
        var publisher = new SqsPublisher(mock.Object, Url);
        await Telemetry.PublishAsync(
            "urn:babel:orders:created", null, env => publisher.PublishEnvelopeAsync(env));

        var request = captured()!;
        var body = request.MessageBody;
        var traceparent = request.MessageAttributes.TryGetValue("traceparent", out var attr) ? attr.StringValue : null;
        Assert.Equal("default", EnvelopeCodec.Decode(body).Meta!.Queue);
        Assert.Null(traceparent);
    }
}
