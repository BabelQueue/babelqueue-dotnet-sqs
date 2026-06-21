using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using BabelQueue;
using BabelQueue.Sqs;
using Moq;
using Xunit;

namespace BabelQueue.Sqs.Tests;

/// <summary>
/// ADR-0028 out-of-band header carrier for SQS: a W3C <c>traceparent</c> rides as a String
/// <c>MessageAttribute</c> beside the frozen envelope (the body is unchanged, <c>schema_version</c>
/// stays 1), never clobbering a contract <c>bq-*</c> attribute, bounded by the 10-attribute SQS
/// limit, and surfaces back on the consume side for <c>Telemetry.Wrap(handler, headers)</c>.
/// No AWS, no network.
/// </summary>
public sealed class SqsHeadersTests
{
    private const string Url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";
    private const string Traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

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
    public async Task PublishWithHeadersRidesTraceparentAsStringAttributeBesideTheEnvelope()
    {
        var (mock, captured) = MockSqs();
        var headers = new Dictionary<string, string> { ["traceparent"] = Traceparent };

        var id = await new SqsPublisher(mock.Object, Url)
            .PublishWithHeadersAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 1 }, headers);

        var sent = captured()!;

        // The traceparent rides as a String attribute beside the bq-* projection.
        Assert.Equal(Traceparent, sent.MessageAttributes["traceparent"].StringValue);
        Assert.Equal("String", sent.MessageAttributes["traceparent"].DataType);

        // GR-1: the wire body is untouched — no traceparent inside the envelope, schema_version stays 1.
        using var body = JsonDocument.Parse(sent.MessageBody);
        Assert.Equal(id, body.RootElement.GetProperty("meta").GetProperty("id").GetString());
        Assert.Equal(1, body.RootElement.GetProperty("meta").GetProperty("schema_version").GetInt32());
        Assert.False(body.RootElement.TryGetProperty("traceparent", out _));
        Assert.False(body.RootElement.GetProperty("meta").TryGetProperty("traceparent", out _));
    }

    [Fact]
    public async Task HeaderLessPublishAddsNoAttributesBeyondThePlainProjection()
    {
        // The body's meta.id is freshly minted per publish, so two Make() calls can never be
        // byte-identical; the header-less invariant is that PublishWithHeadersAsync adds no
        // attribute beyond the §3 bq-* projection that plain PublishAsync already emits.
        var (mockA, capturedA) = MockSqs();
        var (mockB, capturedB) = MockSqs();
        var data = new Dictionary<string, object?> { ["order_id"] = 7 };
        const string trace = "11111111-2222-3333-4444-555555555555";

        await new SqsPublisher(mockA.Object, Url).PublishAsync("urn:x:y", data, traceId: trace);
        await new SqsPublisher(mockB.Object, Url).PublishWithHeadersAsync("urn:x:y", data, headers: null, traceId: trace);

        var plain = capturedA()!;
        var headerLess = capturedB()!;
        Assert.Equal(
            plain.MessageAttributes.Keys.OrderBy(static k => k, StringComparer.Ordinal),
            headerLess.MessageAttributes.Keys.OrderBy(static k => k, StringComparer.Ordinal));
        Assert.DoesNotContain("traceparent", headerLess.MessageAttributes.Keys);
    }

    [Fact]
    public void MergeNeverClobbersAContractAttribute()
    {
        var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 }, "orders");
        var projected = SqsAttributes.Project(env);
        var contractJob = projected["bq-job"].StringValue;

        // An out-of-band header trying to overwrite bq-job must lose to the contract projection.
        var merged = SqsHeaders.Merge(projected, new Dictionary<string, string>
        {
            ["bq-job"] = "urn:evil:override",
            ["traceparent"] = Traceparent,
        });

        Assert.Equal(contractJob, merged["bq-job"].StringValue);
        Assert.Equal(Traceparent, merged["traceparent"].StringValue);
    }

    [Fact]
    public void MergeRespectsTheTenAttributeCeilingAndSkipsEmpty()
    {
        var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 }, "orders");
        var projected = SqsAttributes.Project(env); // 6 contract attributes
        var headers = new Dictionary<string, string> { [""] = "blank-key", ["blank-value"] = "" };
        for (var i = 0; i < 12; i++)
        {
            headers[$"h{i:D2}"] = $"v{i}";
        }

        var merged = SqsHeaders.Merge(projected, headers);

        // Never exceed the SQS 10-attribute ceiling; contract attributes are preserved first.
        Assert.True(merged.Count <= SqsHeaders.MaxMessageAttributes);
        Assert.Equal(SqsHeaders.MaxMessageAttributes, merged.Count);
        Assert.DoesNotContain("", merged.Keys);
        Assert.DoesNotContain("blank-value", merged.Keys);
        Assert.Equal("urn:babel:orders:created", merged["bq-job"].StringValue);
    }

    [Fact]
    public void ExtractSurfacesInboundAttributesAndSkipsEmpty()
    {
        var attributes = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal)
        {
            ["traceparent"] = new() { DataType = "String", StringValue = Traceparent },
            ["bq-job"] = new() { DataType = "String", StringValue = "urn:babel:orders:created" },
            ["empty"] = new() { DataType = "String", StringValue = "" },
        };

        var headers = SqsHeaders.Extract(attributes);

        Assert.Equal(Traceparent, headers["traceparent"]);
        Assert.Equal("urn:babel:orders:created", headers["bq-job"]);
        Assert.DoesNotContain("empty", headers.Keys);
    }

    [Fact]
    public void ExtractOfNullOrEmptyYieldsEmptyMap()
    {
        Assert.Empty(SqsHeaders.Extract(null));
        Assert.Empty(SqsHeaders.Extract(new Dictionary<string, MessageAttributeValue>()));
    }

    [Fact]
    public void ExtractedHeadersRebuildTheRemoteParentForTelemetryWrap()
    {
        // End-to-end seam: produce-side traceparent -> SQS attribute -> Extract -> Telemetry parent.
        var attributes = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal)
        {
            ["traceparent"] = new() { DataType = "String", StringValue = Traceparent },
        };

        var parent = BabelQueue.Tracing.Traceparent.RemoteParentFromHeaders(SqsHeaders.Extract(attributes));

        Assert.NotNull(parent);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", parent!.Value.TraceId.ToHexString());
        Assert.Equal("b7ad6b7169203331", parent.Value.SpanId.ToHexString());
    }
}
