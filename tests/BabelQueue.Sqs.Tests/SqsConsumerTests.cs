using Amazon.SQS;
using Amazon.SQS.Model;
using BabelQueue;
using BabelQueue.Sqs;
using Moq;
using Xunit;

namespace BabelQueue.Sqs.Tests;

public sealed class SqsConsumerTests
{
    private const string Url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

    private static Mock<IAmazonSQS> MockReceiving(Message? message)
    {
        var mock = new Mock<IAmazonSQS>();
        mock.Setup(c => c.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = message is null ? new List<Message>() : new List<Message> { message },
            });
        mock.Setup(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteMessageResponse());
        return mock;
    }

    private static Message Seed(string body, int receiveCount) => new()
    {
        Body = body,
        ReceiptHandle = "rh-1",
        Attributes = new Dictionary<string, string> { ["ApproximateReceiveCount"] = receiveCount.ToString(System.Globalization.CultureInfo.InvariantCulture) },
    };

    private static string Envelope(int attempts = 0)
    {
        var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, "orders");
        return EnvelopeCodec.Encode(env with { Attempts = attempts });
    }

    [Fact]
    public async Task RoutesValidMessageThenDeletes()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        Envelope? seen = null;

        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (env, _, _) => { seen = env; return Task.CompletedTask; },
        };
        var processed = await new SqsConsumer(mock.Object, Url, handlers).PollAsync();

        Assert.Equal(1, processed);
        Assert.Equal("urn:babel:orders:created", seen!.Job);
        mock.Verify(c => c.DeleteMessageAsync(It.Is<DeleteMessageRequest>(r => r.ReceiptHandle == "rh-1"), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReconcilesAttemptsFromReceiveCount()
    {
        var mock = MockReceiving(Seed(Envelope(), 3)); // 3rd delivery -> attempts 2
        var attempts = -1;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (env, _, _) => { attempts = env.Attempts; return Task.CompletedTask; },
        };
        await new SqsConsumer(mock.Object, Url, handlers).PollAsync();
        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task NeverLowersRuntimeAttempts()
    {
        var mock = MockReceiving(Seed(Envelope(attempts: 5), 1));
        var attempts = -1;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (env, _, _) => { attempts = env.Attempts; return Task.CompletedTask; },
        };
        await new SqsConsumer(mock.Object, Url, handlers).PollAsync();
        Assert.Equal(5, attempts);
    }

    [Fact]
    public async Task ThrowingHandlerReleasesViaChangeVisibilityAndReportsOnError()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        Exception? captured = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => throw new InvalidOperationException("boom"),
        };
        var options = new SqsConsumerOptions { OnError = (e, _, _) => captured = e };

        await new SqsConsumer(mock.Object, Url, handlers, options).PollAsync();

        Assert.IsType<InvalidOperationException>(captured);
        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        mock.Verify(c => c.ChangeMessageVisibilityAsync(
            It.Is<ChangeMessageVisibilityRequest>(r => r.QueueUrl == Url && r.ReceiptHandle == "rh-1" && r.VisibilityTimeout == 0),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData(30, 30)]
    [InlineData(-5, 0)]
    [InlineData(99999, 43200)]
    public async Task ReleaseBackoffIsClampedToSqsVisibilityRange(int backoff, int expected)
    {
        var mock = MockReceiving(Seed(Envelope(), 2));
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => throw new InvalidOperationException("boom"),
        };
        var options = new SqsConsumerOptions { RetryBackoffSeconds = backoff };

        await new SqsConsumer(mock.Object, Url, handlers, options).PollAsync();

        mock.Verify(c => c.ChangeMessageVisibilityAsync(
            It.Is<ChangeMessageVisibilityRequest>(r => r.VisibilityTimeout == expected),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReleaseWithoutReceiptHandleIsSkipped()
    {
        var message = Seed(Envelope(), 1);
        message.ReceiptHandle = null;
        var mock = MockReceiving(message);
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => throw new InvalidOperationException("boom"),
        };

        await new SqsConsumer(mock.Object, Url, handlers).PollAsync();

        mock.Verify(c => c.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task UnknownUrnReleaseStrategyChangesVisibility()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        var options = new SqsConsumerOptions
        {
            UnknownUrnStrategy = UnknownUrnStrategy.Release,
            UnknownUrnReleaseDelaySeconds = 12,
        };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        mock.Verify(c => c.ChangeMessageVisibilityAsync(
            It.Is<ChangeMessageVisibilityRequest>(r => r.ReceiptHandle == "rh-1" && r.VisibilityTimeout == 12),
            It.IsAny<CancellationToken>()), Times.Once);
        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task UnknownUrnReleaseDefaultsToZeroSeconds()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        var options = new SqsConsumerOptions { UnknownUrnStrategy = UnknownUrnStrategy.Release };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        mock.Verify(c => c.ChangeMessageVisibilityAsync(
            It.Is<ChangeMessageVisibilityRequest>(r => r.VisibilityTimeout == 0),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UnknownUrnDeleteStrategyDeletes()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        var options = new SqsConsumerOptions { UnknownUrnStrategy = UnknownUrnStrategy.Delete };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        mock.Verify(c => c.DeleteMessageAsync(It.Is<DeleteMessageRequest>(r => r.ReceiptHandle == "rh-1"), It.IsAny<CancellationToken>()), Times.Once);
        mock.Verify(c => c.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task UnknownUrnDeadLetterStrategySendsToDlqThenDeletes()
    {
        const string dlq = Url + ".dlq";
        var mock = MockReceiving(Seed(Envelope(), 1));
        SendMessageRequest? sent = null;
        mock.Setup(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<SendMessageRequest, CancellationToken>((r, _) => sent = r)
            .ReturnsAsync(new SendMessageResponse());
        var options = new SqsConsumerOptions
        {
            UnknownUrnStrategy = UnknownUrnStrategy.DeadLetter,
            DeadLetterQueueUrl = dlq,
        };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.NotNull(sent);
        Assert.Equal(dlq, sent!.QueueUrl);
        var dead = EnvelopeCodec.Decode(sent.MessageBody);
        Assert.Equal("unknown_urn", dead.DeadLetter!.Reason);
        Assert.Equal("orders", dead.DeadLetter.OriginalQueue);
        Assert.Equal("urn:babel:orders:created", sent.MessageAttributes["bq-job"].StringValue);
        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UnknownUrnDeadLetterWithoutDlqDegradesToDelete()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        var options = new SqsConsumerOptions { UnknownUrnStrategy = UnknownUrnStrategy.DeadLetter };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        mock.Verify(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DeadLetteredMessageCarriesUnknownKeys()
    {
        const string dlq = Url + ".dlq";
        var body = Envelope().Replace("\"attempts\":0", "\"attempts\":0,\"extra_top\":{\"k\":[1,2]}", StringComparison.Ordinal);
        var mock = MockReceiving(Seed(body, 1));
        SendMessageRequest? sent = null;
        mock.Setup(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<SendMessageRequest, CancellationToken>((r, _) => sent = r)
            .ReturnsAsync(new SendMessageResponse());
        var options = new SqsConsumerOptions { UnknownUrnStrategy = UnknownUrnStrategy.DeadLetter, DeadLetterQueueUrl = dlq };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.Contains("\"extra_top\":{\"k\":[1,2]}", sent!.MessageBody, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NonConformantEnvelopeReportsOnError()
    {
        var mock = MockReceiving(Seed("{\"not\":\"an envelope\"}", 1));
        Exception? captured = null;
        var options = new SqsConsumerOptions { OnError = (e, _, _) => captured = e };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.IsType<BabelQueueException>(captured);
        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task UnknownUrnCallsHandlerThenDeletesOrReportsOnError()
    {
        var withHook = MockReceiving(Seed(Envelope(), 1));
        string? unknown = null;
        var optionsA = new SqsConsumerOptions { OnUnknownUrn = (env, _, _) => { unknown = EnvelopeCodec.Urn(env); return Task.CompletedTask; } };
        await new SqsConsumer(withHook.Object, Url, new Dictionary<string, BabelHandler>(), optionsA).PollAsync();
        Assert.Equal("urn:babel:orders:created", unknown);
        withHook.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Once);

        var noHook = MockReceiving(Seed(Envelope(), 1));
        Exception? captured = null;
        var optionsB = new SqsConsumerOptions { OnError = (e, _, _) => captured = e };
        await new SqsConsumer(noHook.Object, Url, new Dictionary<string, BabelHandler>(), optionsB).PollAsync();
        Assert.IsType<UnknownUrnException>(captured);
        noHook.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task PollPassesContractReceiveOptions()
    {
        ReceiveMessageRequest? captured = null;
        var mock = new Mock<IAmazonSQS>();
        mock.Setup(c => c.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<ReceiveMessageRequest, CancellationToken>((req, _) => captured = req)
            .ReturnsAsync(new ReceiveMessageResponse { Messages = new List<Message>() });

        var options = new SqsConsumerOptions { WaitTimeSeconds = 5, VisibilityTimeout = 45, MaxMessages = 3 };
        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.Equal(5, captured!.WaitTimeSeconds);
        Assert.Equal(45, captured.VisibilityTimeout);
        Assert.Equal(3, captured.MaxNumberOfMessages);
        Assert.Equal(new List<string> { "All" }, captured.MessageAttributeNames);
        Assert.Equal(new List<string> { "ApproximateReceiveCount" }, captured.MessageSystemAttributeNames);
    }

    [Fact]
    public async Task RunStopsWhenCancelled()
    {
        var mock = MockReceiving(null);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>()).RunAsync(cts.Token);

        mock.Verify(c => c.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task EmptyPollReturnsZero()
    {
        var mock = MockReceiving(null);
        var processed = await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>()).PollAsync();
        Assert.Equal(0, processed);
    }

    private static Message SeedWith(string body, string receiptHandle) => new()
    {
        Body = body,
        ReceiptHandle = receiptHandle,
        Attributes = new Dictionary<string, string> { ["ApproximateReceiveCount"] = "1" },
    };

    [Fact]
    public async Task FailingReleaseIsReportedAndTheBatchContinues()
    {
        var mock = new Mock<IAmazonSQS>();
        mock.Setup(c => c.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = new List<Message> { SeedWith(Envelope(), "rh-bad"), SeedWith(Envelope(), "rh-good") },
            });
        mock.Setup(c => c.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ReceiptHandleIsInvalidException("expired"));
        mock.Setup(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteMessageResponse());
        var errors = new List<Exception>();
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => ++calls == 1
                ? throw new InvalidOperationException("boom")
                : Task.CompletedTask,
        };
        var options = new SqsConsumerOptions { OnError = (e, _, _) => errors.Add(e) };

        var processed = await new SqsConsumer(mock.Object, Url, handlers, options).PollAsync();

        Assert.Equal(2, processed);
        Assert.Equal(2, calls);
        Assert.IsType<InvalidOperationException>(errors[0]);
        Assert.IsType<ReceiptHandleIsInvalidException>(errors[1]);
        mock.Verify(c => c.DeleteMessageAsync(It.Is<DeleteMessageRequest>(r => r.ReceiptHandle == "rh-good"), It.IsAny<CancellationToken>()), Times.Once);
        mock.Verify(c => c.DeleteMessageAsync(It.Is<DeleteMessageRequest>(r => r.ReceiptHandle == "rh-bad"), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task FailingUnknownUrnReleaseIsReported()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        mock.Setup(c => c.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonSQSException("throttled"));
        Exception? captured = null;
        var options = new SqsConsumerOptions
        {
            UnknownUrnStrategy = UnknownUrnStrategy.Release,
            OnError = (e, _, _) => captured = e,
        };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.IsType<AmazonSQSException>(captured);
    }

    [Fact]
    public async Task FailingDeadLetterSendIsReportedAndMessageNotDeleted()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        mock.Setup(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonSQSException("MissingParameter"));
        Exception? captured = null;
        var options = new SqsConsumerOptions
        {
            UnknownUrnStrategy = UnknownUrnStrategy.DeadLetter,
            DeadLetterQueueUrl = Url + ".dlq",
            OnError = (e, _, _) => captured = e,
        };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.IsType<AmazonSQSException>(captured);
        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ThrowingHandlerOnShutdownSkipsRelease()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        using var cts = new CancellationTokenSource();
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = async (_, _, ct) =>
            {
                await cts.CancelAsync();
                ct.ThrowIfCancellationRequested();
            },
        };

        await new SqsConsumer(mock.Object, Url, handlers).PollAsync(cts.Token);

        mock.Verify(c => c.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task FifoDeadLetterQueueGetsGroupAndDeduplicationIds()
    {
        const string dlq = Url + ".dlq.fifo";
        var mock = MockReceiving(Seed(Envelope(), 1));
        SendMessageRequest? sent = null;
        mock.Setup(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<SendMessageRequest, CancellationToken>((r, _) => sent = r)
            .ReturnsAsync(new SendMessageResponse());
        var options = new SqsConsumerOptions { UnknownUrnStrategy = UnknownUrnStrategy.DeadLetter, DeadLetterQueueUrl = dlq };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.NotNull(sent);
        Assert.Equal("orders", sent!.MessageGroupId);
        Assert.False(string.IsNullOrEmpty(sent.MessageDeduplicationId));
        Assert.Equal(EnvelopeCodec.Decode(sent.MessageBody).Meta!.Id, sent.MessageDeduplicationId);
    }

    [Fact]
    public async Task StandardDeadLetterQueueHasNoGroupId()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        SendMessageRequest? sent = null;
        mock.Setup(c => c.SendMessageAsync(It.IsAny<SendMessageRequest>(), It.IsAny<CancellationToken>()))
            .Callback<SendMessageRequest, CancellationToken>((r, _) => sent = r)
            .ReturnsAsync(new SendMessageResponse());
        var options = new SqsConsumerOptions { UnknownUrnStrategy = UnknownUrnStrategy.DeadLetter, DeadLetterQueueUrl = Url + ".dlq" };

        await new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.Null(sent!.MessageGroupId);
        Assert.Null(sent.MessageDeduplicationId);
    }

    [Fact]
    public async Task FailingDeleteAfterSuccessfulHandlerIsReportedDistinctlyAndNotReleased()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        var throttled = new AmazonSQSException("RequestThrottled");
        mock.Setup(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(throttled);
        var errors = new List<Exception>();
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };
        var options = new SqsConsumerOptions { OnError = (e, _, _) => errors.Add(e) };

        var processed = await new SqsConsumer(mock.Object, Url, handlers, options).PollAsync();

        Assert.Equal(1, processed);
        Assert.Equal(1, calls);
        var error = Assert.Single(errors);
        Assert.IsType<BabelQueueException>(error);
        Assert.Same(throttled, error.InnerException);
        mock.Verify(c => c.ChangeMessageVisibilityAsync(It.IsAny<ChangeMessageVisibilityRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task StrategyIsSnapshottedAtConstruction()
    {
        var mock = MockReceiving(Seed(Envelope(), 1));
        Exception? captured = null;
        var options = new SqsConsumerOptions { OnError = (e, _, _) => captured = e };
        var consumer = new SqsConsumer(mock.Object, Url, new Dictionary<string, BabelHandler>(), options);

        options.UnknownUrnStrategy = UnknownUrnStrategy.Delete;
        await consumer.PollAsync();

        Assert.IsType<UnknownUrnException>(captured);
        mock.Verify(c => c.DeleteMessageAsync(It.IsAny<DeleteMessageRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Theory]
    [InlineData("dead-letter")]
    [InlineData("")]
    [InlineData("FAIL")]
    public void UnrecognisedUnknownUrnStrategyIsRejected(string strategy)
    {
        var options = new SqsConsumerOptions { UnknownUrnStrategy = strategy };

        Assert.Throws<ArgumentException>(
            () => new SqsConsumer(new Mock<IAmazonSQS>().Object, Url, new Dictionary<string, BabelHandler>(), options));
    }
}
