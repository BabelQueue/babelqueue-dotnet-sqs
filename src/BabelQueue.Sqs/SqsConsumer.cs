using System.Globalization;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace BabelQueue.Sqs;

/// <summary>
/// Polls an SQS queue, decodes and validates each message, routes it to the handler
/// registered for its URN, and deletes it on success. A throwing handler leaves the
/// message undeleted — SQS redelivers it after the visibility timeout (at-least-once);
/// <c>attempts</c> is reconciled to <c>ApproximateReceiveCount - 1</c> for the handler.
/// The poll loop never stops on a bad message — observe via the option hooks.
/// </summary>
public sealed class SqsConsumer
{
    private const string ReceiveCountAttribute = "ApproximateReceiveCount";

    private readonly IAmazonSQS _client;
    private readonly string _queueUrl;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly SqsConsumerOptions _options;

    public SqsConsumer(
        IAmazonSQS client,
        string queueUrl,
        IReadOnlyDictionary<string, BabelHandler> handlers,
        SqsConsumerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrEmpty(queueUrl);
        ArgumentNullException.ThrowIfNull(handlers);
        _client = client;
        _queueUrl = queueUrl;
        _handlers = handlers;
        _options = options ?? new SqsConsumerOptions();
    }

    /// <summary>Receive one batch, route each message, delete the ones handled. Returns the batch size.</summary>
    public async Task<int> PollAsync(CancellationToken cancellationToken = default)
    {
        var request = new ReceiveMessageRequest
        {
            QueueUrl = _queueUrl,
            MaxNumberOfMessages = _options.MaxMessages,
            WaitTimeSeconds = _options.WaitTimeSeconds,
            MessageAttributeNames = new List<string> { "All" },
            MessageSystemAttributeNames = new List<string> { ReceiveCountAttribute },
        };
        if (_options.VisibilityTimeout is int visibility)
        {
            request.VisibilityTimeout = visibility;
        }

        var response = await _client.ReceiveMessageAsync(request, cancellationToken).ConfigureAwait(false);
        var messages = response.Messages ?? new List<Message>();
        foreach (var message in messages)
        {
            await HandleAsync(message, cancellationToken).ConfigureAwait(false);
        }

        return messages.Count;
    }

    /// <summary>Poll until <paramref name="cancellationToken"/> is cancelled (each poll long-polls).</summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await PollAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HandleAsync(Message message, CancellationToken cancellationToken)
    {
        var envelope = Reconcile(
            EnvelopeCodec.Decode(message.Body ?? string.Empty),
            ReceiveCount(message));

        if (!EnvelopeCodec.Accepts(envelope))
        {
            _options.OnError?.Invoke(
                new BabelQueueException("Rejected a non-conformant BabelQueue envelope from SQS."),
                envelope, message);
            return;
        }

        var urn = EnvelopeCodec.Urn(envelope);
        if (!_handlers.TryGetValue(urn, out var handler))
        {
            if (_options.OnUnknownUrn is not null)
            {
                await _options.OnUnknownUrn(envelope, message, cancellationToken).ConfigureAwait(false);
                await DeleteAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                _options.OnError?.Invoke(new UnknownUrnException(urn), envelope, message);
            }
            return;
        }

        try
        {
            await handler(envelope, message, cancellationToken).ConfigureAwait(false);
            await DeleteAsync(message, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The consume loop must survive any handler exception.
        catch (Exception error)
#pragma warning restore CA1031
        {
            // Leave the message undeleted — SQS redelivers after the visibility timeout.
            _options.OnError?.Invoke(error, envelope, message);
        }
    }

    private static string? ReceiveCount(Message message)
        => message.Attributes is not null && message.Attributes.TryGetValue(ReceiveCountAttribute, out var value)
            ? value
            : null;

    /// <summary>
    /// Sets <c>attempts</c> to max(current, ApproximateReceiveCount - 1): a first delivery
    /// reads 0, a natively-redelivered message reflects its true count, and a
    /// runtime-incremented counter is never lowered.
    /// </summary>
    private static Envelope Reconcile(Envelope envelope, string? receiveCount)
    {
        if (receiveCount is null
            || !int.TryParse(receiveCount, NumberStyles.Integer, CultureInfo.InvariantCulture, out var count)
            || count <= 1)
        {
            return envelope;
        }

        var native = count - 1;
        return native > envelope.Attempts ? envelope with { Attempts = native } : envelope;
    }

    private async Task DeleteAsync(Message message, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(message.ReceiptHandle))
        {
            return;
        }

        await _client.DeleteMessageAsync(
            new DeleteMessageRequest { QueueUrl = _queueUrl, ReceiptHandle = message.ReceiptHandle },
            cancellationToken).ConfigureAwait(false);
    }
}
