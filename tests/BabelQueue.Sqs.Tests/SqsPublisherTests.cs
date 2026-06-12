using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using BabelQueue;
using BabelQueue.Sqs;
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
}
