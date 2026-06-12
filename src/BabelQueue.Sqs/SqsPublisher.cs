using Amazon.SQS;
using Amazon.SQS.Model;

namespace BabelQueue.Sqs;

/// <summary>
/// Sends canonical-envelope messages to one SQS queue with the §3 attribute projection
/// (<c>bq-job</c>/<c>bq-trace-id</c>/<c>bq-message-id</c>/<c>bq-schema-version</c>/
/// <c>bq-source-lang</c>/<c>bq-created-at</c>), so a Go/Python/... consumer can route and
/// trace without decoding the body. The envelope is unchanged (<c>schema_version</c>
/// stays 1); SQS is purely additive.
/// </summary>
public sealed class SqsPublisher
{
    private readonly IAmazonSQS _client;
    private readonly string _queueUrl;
    private readonly bool _fifo;
    private readonly string? _messageGroupId;
    private readonly bool _contentDedup;

    /// <param name="client">The AWS SQS client (any <see cref="IAmazonSQS"/> — mockable in tests).</param>
    /// <param name="queueUrl">The target queue URL.</param>
    /// <param name="fifo">Treat the queue as FIFO (its name must end in <c>.fifo</c>).</param>
    /// <param name="messageGroupId">FIFO ordering group (default: the queue name from the URL).</param>
    /// <param name="contentDedup">Use the queue's content-based dedup instead of <c>meta.id</c>.</param>
    public SqsPublisher(
        IAmazonSQS client,
        string queueUrl,
        bool fifo = false,
        string? messageGroupId = null,
        bool contentDedup = false)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(queueUrl);
        _client = client;
        _queueUrl = queueUrl;
        _fifo = fifo;
        _messageGroupId = messageGroupId;
        _contentDedup = contentDedup;
    }

    /// <summary>
    /// Builds the canonical envelope for <c>(urn, data)</c>, sends it as the message body
    /// with the projected attributes, and returns the message id (<c>meta.id</c>).
    /// </summary>
    public async Task<string> PublishAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data = null,
        string? traceId = null,
        CancellationToken cancellationToken = default)
    {
        var envelope = EnvelopeCodec.Make(urn, data, QueueName(_queueUrl), traceId);

        var request = new SendMessageRequest
        {
            QueueUrl = _queueUrl,
            MessageBody = EnvelopeCodec.Encode(envelope),
            MessageAttributes = SqsAttributes.Project(envelope),
        };

        if (_fifo)
        {
            request.MessageGroupId = _messageGroupId ?? QueueName(_queueUrl);
            if (!_contentDedup && !string.IsNullOrEmpty(envelope.Meta?.Id))
            {
                request.MessageDeduplicationId = envelope.Meta.Id;
            }
        }

        await _client.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);

        return envelope.Meta?.Id ?? string.Empty;
    }

    private static string QueueName(string queueUrl)
    {
        var segments = queueUrl.Split('/', StringSplitOptions.RemoveEmptyEntries);
        return segments.Length == 0 ? "default" : segments[^1];
    }
}
