using System.Text;
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

    [Fact]
    public async Task SchemaVersionGateMatchesGolden()
    {
        var gate = Sqs().GetProperty("schema_version_gate");
        var property = gate.GetProperty("property").GetString()!;
        var body = File.ReadAllText(Path.Combine(Dir, gate.GetProperty("fixture").GetString()!));
        var job = EnvelopeCodec.Decode(body).Job!;

        var cases = gate.GetProperty("cases").EnumerateArray().ToList();
        Assert.NotEmpty(cases);
        foreach (var testCase in cases)
        {
            var absent = testCase.TryGetProperty("absent", out var a) && a.GetBoolean();
            var value = absent ? null : testCase.GetProperty("value").GetString();
            var expect = testCase.GetProperty("expect").GetString();

            var message = new Message { Body = body, ReceiptHandle = "rh" };
            if (!absent)
            {
                message.MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    [property] = new() { DataType = "String", StringValue = value },
                };
            }

            var mock = new Mock<IAmazonSQS>();
            mock.Setup(s => s.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ReceiveMessageResponse { Messages = new List<Message> { message } });
            mock.Setup(s => s.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeleteMessageResponse());

            var seen = 0;
            var handlers = new Dictionary<string, BabelHandler>
            {
                [job] = (_, _, _) => { seen++; return Task.CompletedTask; },
            };
            await new SqsConsumer(mock.Object, Url, handlers).PollAsync();

            var label = absent ? "<absent>" : $"'{value}'";
            Assert.True(
                (expect == "decode" ? 1 : 0) == seen,
                $"{label}: expected {expect} but handler ran {seen} time(s)");
            mock.Verify(
                c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()),
                expect == "decode" ? Times.Once() : Times.Never());
        }
    }

    // -- poison_dlq (ADR-0035) --------------------------------------------------------------------------------------

    private static JsonElement Section(string name)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty(name).Clone();
    }

    private static readonly JsonDocumentOptions DeepJson = new() { MaxDepth = 10_100 };

    [Fact]
    public void PoisonDlqRecordShapeMatchesGolden()
    {
        var section = Section("poison_dlq");
        var originalQueue = section.GetProperty("original_queue").GetString()!;
        var cases = section.GetProperty("cases").EnumerateArray().ToList();
        Assert.Equal(43, cases.Count);
        foreach (var testCase in cases)
        {
            var name = testCase.GetProperty("name").GetString();
            var bytes = testCase.TryGetProperty("body_base64", out var b64)
                ? Convert.FromBase64String(b64.GetString()!)
                : Encoding.UTF8.GetBytes(testCase.GetProperty("body").GetString()!);
            var expect = testCase.GetProperty("expect");
            var shape = expect.GetProperty("shape").GetString()!;

            var record = SqsConsumer.PoisonBody(bytes, "poison", "System.Exception", 1700000000000L, originalQueue, 0);
            var text = Encoding.UTF8.GetString(record);
            Assert.False(text.Contains('\uFFFD', StringComparison.Ordinal), $"{name}: the record holds U+FFFD");

            using var doc = JsonDocument.Parse(record, DeepJson);
            Assert.Equal(JsonValueKind.Object, doc.RootElement.ValueKind);
            var deadLetter = doc.RootElement.EnumerateObject().Where(p => p.Name == "dead_letter").ToList();
            Assert.True(deadLetter.Count == 1, $"{name}: exactly one top-level dead_letter");
            Assert.Equal("poison", deadLetter[0].Value.GetProperty("reason").GetString());
            Assert.Equal(originalQueue, deadLetter[0].Value.GetProperty("original_queue").GetString());
            var rest = doc.RootElement.EnumerateObject().Where(p => p.Name != "dead_letter").ToList();

            var want = expect.GetProperty("record");
            if (shape == "splice_or_wrapper" && !SameMembers(rest, want))
            {
                foreach (var literal in expect.GetProperty("splice_literals").EnumerateArray())
                {
                    Assert.True(text.Contains(literal.GetString()!, StringComparison.Ordinal), $"{name}: missing literal {literal.GetString()}");
                }

                Assert.DoesNotContain(rest, p => p.Name is "raw" or "raw_encoding");
                var keys = rest.Select(p => p.Name).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToList();
                var wantKeys = expect.GetProperty("splice_keys").EnumerateArray().Select(k => k.GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToList();
                Assert.Equal(wantKeys, keys);
                continue;
            }

            Assert.True(SameMembers(rest, want), $"{name}: the record without dead_letter differs: {text}");
        }
    }

    private static bool SameMembers(List<JsonProperty> members, JsonElement want)
    {
        var wantMembers = want.EnumerateObject().ToList();
        return members.Count == wantMembers.Count
            && members.All(m => wantMembers.Count(w => w.Name == m.Name && SameJson(m.Value, w.Value)) == 1);
    }

    private static bool SameJson(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
        {
            return false;
        }

        return a.ValueKind switch
        {
            JsonValueKind.Object => SameMembers(a.EnumerateObject().ToList(), b),
            JsonValueKind.Array => a.GetArrayLength() == b.GetArrayLength()
                && a.EnumerateArray().Zip(b.EnumerateArray(), SameJson).All(x => x),
            JsonValueKind.String => a.GetString() == b.GetString(),
            JsonValueKind.Number => a.GetRawText() == b.GetRawText(),
            _ => true,
        };
    }

    // -- version_gate_sqs (ADR-0037 decision 1) ---------------------------------------------------------------------

    [Fact]
    public async Task VersionGateSettleMatchesGolden()
    {
        const string dlqUrl = Url + ".dlq";
        var section = Section("version_gate_sqs");
        var cases = section.GetProperty("cases").EnumerateArray().ToList();
        Assert.Equal(17, cases.Count);
        var bodies = section.GetProperty("incoming_bodies").EnumerateArray()
            .Select(b => (Name: b.GetProperty("name").GetString()!, Text: b.GetProperty("body").GetString()!))
            .ToList();
        Assert.Equal(3, bodies.Count);

        foreach (var testCase in cases)
        {
            var caseName = testCase.GetProperty("name").GetString();
            var deadLetterOn = testCase.GetProperty("dead_letter").GetString() == "on";
            var writeFails = testCase.TryGetProperty("dlq_write", out var w) && w.GetString() == "fails";
            var expect = testCase.GetProperty("expect");
            // `off` means no DLQ URL in this consumer, so outcome_no_writer (where present) is the expectation there.
            var outcome = !deadLetterOn && expect.TryGetProperty("outcome_no_writer", out var nw)
                ? nw.GetString()!
                : expect.GetProperty("outcome").GetString()!;

            foreach (var (bodyName, bodyText) in bodies)
            {
                var label = $"{caseName} / {bodyName}";
                var message = new Message
                {
                    Body = bodyText,
                    ReceiptHandle = "rh",
                    Attributes = new Dictionary<string, string> { ["ApproximateReceiveCount"] = "9" },
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        ["bq-schema-version"] = new() { DataType = "String", StringValue = "2" },
                    },
                };
                if (testCase.TryGetProperty("bq_attempts", out var attempts))
                {
                    message.MessageAttributes["bq-attempts"] = new MessageAttributeValue { DataType = "String", StringValue = attempts.GetString() };
                }

                var mock = new Mock<IAmazonSQS>();
                mock.Setup(s => s.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new ReceiveMessageResponse { Messages = new List<Message> { message } });
                mock.Setup(s => s.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new DeleteMessageResponse());
                var sent = new List<SendMessageRequest>();
                var send = mock.Setup(s => s.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()));
                if (writeFails)
                {
                    send.ThrowsAsync(new AmazonSQSException("dlq down"));
                }
                else
                {
                    send.Callback<SendMessageRequest, CancellationToken>((r, _) => sent.Add(r)).ReturnsAsync(new SendMessageResponse());
                }

                var handled = 0;
                var handlers = new Dictionary<string, BabelHandler>
                {
                    ["urn:babel:orders:created"] = (_, _, _) => { handled++; return Task.CompletedTask; },
                };
                var options = new SqsConsumerOptions { DeadLetterQueueUrl = deadLetterOn ? dlqUrl : null };
                if (testCase.TryGetProperty("on_unknown_urn", out var strategy))
                {
                    options.UnknownUrnStrategy = strategy.GetString()!;
                }

                await new SqsConsumer(mock.Object, Url, handlers, options).PollAsync();

                Assert.True(handled == 0, $"{label}: a version-gated delivery reached a handler");
                mock.Verify(c => c.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()), Times.Never());
                switch (outcome)
                {
                    case "ack":
                        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Once());
                        Assert.True(sent.Count == 0, $"{label}: ack must not write a record");
                        break;
                    case "dlq_raw":
                        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Once());
                        var record = Assert.Single(sent);
                        Assert.Equal(dlqUrl, record.QueueUrl);
                        Assert.True(string.Equals(bodyText, record.MessageBody, StringComparison.Ordinal), $"{label}: the DLQ record is not the incoming body byte for byte");
                        break;
                    case "unsettled":
                    case "native":
                        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never());
                        if (outcome == "native")
                        {
                            mock.Verify(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never());
                        }

                        break;
                    default:
                        Assert.Fail($"{label}: unknown outcome {outcome}");
                        break;
                }
            }
        }
    }
}
