using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using BabelQueue;
using BabelQueue.Sqs;
using Moq;
using Xunit;

namespace BabelQueue.Sqs.Tests;

/// <summary>
/// Amazon SQS binding conformance against the vendored canonical suite's <c>sqs</c> block:
/// the §3 attribute projection and the <c>attempts = ApproximateReceiveCount - 1</c>
/// reconciliation. No AWS, no network.
/// </summary>
public sealed class SqsConformanceTests
{
    private const string Url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");

    private static JsonElement Sqs()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty("sqs").Clone();
    }

    [Fact]
    public void AttributeProjectionMatchesGolden()
    {
        var projection = Sqs().GetProperty("attribute_projection");
        var body = File.ReadAllText(Path.Combine(Dir, projection.GetProperty("envelope_file").GetString()!));
        var got = SqsAttributes.Project(EnvelopeCodec.Decode(body));
        var want = projection.GetProperty("message_attributes");

        Assert.Equal(want.EnumerateObject().Count(), got.Count);
        foreach (var attr in want.EnumerateObject())
        {
            Assert.True(got.ContainsKey(attr.Name), attr.Name);
            Assert.Equal(attr.Value.GetProperty("DataType").GetString(), got[attr.Name].DataType);
            Assert.Equal(attr.Value.GetProperty("StringValue").GetString(), got[attr.Name].StringValue);
        }
    }

    [Fact]
    public async Task AttemptsReconciliationMatchesGolden()
    {
        foreach (var testCase in Sqs().GetProperty("attempts_reconciliation").GetProperty("cases").EnumerateArray())
        {
            var bodyAttempts = testCase.GetProperty("body_attempts").GetInt32();
            var expected = testCase.GetProperty("expected_attempts").GetInt32();
            var rc = testCase.GetProperty("approximate_receive_count");
            string? receiveCount = rc.ValueKind == JsonValueKind.Null ? null : rc.GetString();

            var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 }, "orders")
                with { Attempts = bodyAttempts };
            var message = new Message
            {
                Body = EnvelopeCodec.Encode(env),
                ReceiptHandle = "rh",
                Attributes = receiveCount is null
                    ? null
                    : new Dictionary<string, string> { ["ApproximateReceiveCount"] = receiveCount },
            };

            var mock = new Mock<IAmazonSQS>();
            mock.Setup(s => s.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ReceiveMessageResponse { Messages = new List<Message> { message } });
            mock.Setup(s => s.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeleteMessageResponse());

            var seen = -1;
            var handlers = new Dictionary<string, BabelHandler>
            {
                ["urn:babel:orders:created"] = (e, _, _) => { seen = e.Attempts; return Task.CompletedTask; },
            };
            await new SqsConsumer(mock.Object, Url, handlers).PollAsync();

            Assert.Equal(expected, seen);
        }
    }
}
