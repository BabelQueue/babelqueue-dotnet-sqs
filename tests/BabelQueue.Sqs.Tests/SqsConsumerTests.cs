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
    public async Task ThrowingHandlerLeavesMessageAndReportsOnError()
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
}
