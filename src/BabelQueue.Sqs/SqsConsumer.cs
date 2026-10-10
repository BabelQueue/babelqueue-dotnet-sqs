using System.Globalization;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace BabelQueue.Sqs;

/// <summary>
/// Polls an SQS queue, decodes and validates each message, routes it to the handler
/// registered for its URN, and deletes it on success. A throwing handler releases the
/// message with <c>ChangeMessageVisibility(VisibilityTimeout = backoff)</c> — it is never
/// deleted, so SQS redelivers it (at-least-once, broker-bindings.md §3.5);
/// <c>attempts</c> is reconciled to <c>ApproximateReceiveCount - 1</c> for the handler.
/// An unmapped URN follows <see cref="SqsConsumerOptions.UnknownUrnStrategy"/>.
/// The poll loop never stops on a bad message — observe via the option hooks: a failing
/// broker call on the failure path (release, dead-letter send, delete of an unknown URN)
/// is reported through <see cref="SqsConsumerOptions.OnError"/> and the message is left
/// to SQS's visibility expiry. A failing delete after a successful handler is reported as a
/// <see cref="BabelQueueException"/> wrapping the broker error and is never released.
/// <para>
/// Per §3.7 the <c>bq-schema-version</c> message attribute is checked <b>before</b> the body is
/// decoded: when it is present and is not exactly the schema version the core supports
/// (<see cref="EnvelopeCodec.SchemaVersion"/>, compared exactly as sent, as its canonical decimal string, with no
/// trimming — so <c>"2"</c>, <c>"x"</c>, <c>"01"</c> and <c>" 1"</c> are all unknown), the body is never decoded or routed;
/// <c>OnError</c> is notified (with an empty, undecoded envelope) and the message is neither deleted
/// nor released, so SQS redelivers it on visibility expiry and its redrive policy moves it to the DLQ
/// — the same path as a non-conformant envelope. A missing or blank (empty or ASCII-whitespace-only: space,
/// tab, LF, VT, FF, CR) attribute changes nothing, and a <c>"1"</c> attribute still goes through the
/// post-decode <see cref="EnvelopeCodec.Accepts"/> check.
/// </para>
/// </summary>
public sealed class SqsConsumer
{
    private const string ReceiveCountAttribute = "ApproximateReceiveCount";
    private const string SchemaVersionAttribute = "bq-schema-version";
    private const int MaxVisibilityTimeoutSeconds = 43200;

    private readonly IAmazonSQS _client;
    private readonly string _queueUrl;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly SqsConsumerOptions _options;
    private readonly string _unknownUrnStrategy;

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
        if (!IsKnownStrategy(_options.UnknownUrnStrategy))
        {
            throw new ArgumentException(
                $"Unknown UnknownUrnStrategy '{_options.UnknownUrnStrategy}'; expected one of "
                + $"'{UnknownUrnStrategy.Fail}', '{UnknownUrnStrategy.Delete}', "
                + $"'{UnknownUrnStrategy.Release}', '{UnknownUrnStrategy.DeadLetter}'.",
                nameof(options));
        }

        // Snapshot: SqsConsumerOptions is mutable, so a later assignment must not bypass validation.
        _unknownUrnStrategy = _options.UnknownUrnStrategy;
    }

    private static bool IsKnownStrategy(string? strategy) => strategy is
        UnknownUrnStrategy.Fail or UnknownUrnStrategy.Delete
        or UnknownUrnStrategy.Release or UnknownUrnStrategy.DeadLetter;

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
        // §3.7: version-gate on the attribute before decoding the body. The value is compared exactly
        // as sent (no trimming), like the Pulsar/Go/Python/Node consumers (GR-5).
        var declaredVersion = SchemaVersionValue(message);
        if (declaredVersion is not null && !IsBlankSchemaVersion(declaredVersion)
            && !string.Equals(
                declaredVersion,
                EnvelopeCodec.SchemaVersion.ToString(CultureInfo.InvariantCulture),
                StringComparison.Ordinal))
        {
            // The body is deliberately not decoded, so hand OnError an empty envelope. The message is
            // neither deleted nor released: SQS redelivers it and the redrive policy dead-letters it.
            _options.OnError?.Invoke(
                new BabelQueueException(
                    $"Rejected an SQS message: unsupported {SchemaVersionAttribute} attribute '{declaredVersion}' "
                    + $"(supported: {EnvelopeCodec.SchemaVersion}); body not decoded."),
                new Envelope(null, null, null, null, 0, null),
                message);
            return;
        }

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
            await GuardAsync(
                () => HandleUnknownUrnAsync(urn, envelope, message, cancellationToken),
                envelope, message, cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            await handler(envelope, message, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The consume loop must survive any handler exception.
        catch (Exception error)
#pragma warning restore CA1031
        {
            // Release, never delete: the message becomes visible again after the backoff
            // and ApproximateReceiveCount advances (broker-bindings.md §3.5).
            // On shutdown the token is cancelled: skip the release, the visibility timeout
            // returns the message anyway.
            _options.OnError?.Invoke(error, envelope, message);
            if (!cancellationToken.IsCancellationRequested)
            {
                await GuardAsync(
                    () => ReleaseAsync(message, _options.RetryBackoffSeconds, cancellationToken),
                    envelope, message, cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        // The handler succeeded: a failing delete is not a handler failure. Report it as a
        // BabelQueueException wrapping the broker error and do NOT release — releasing at the
        // (default 0 s) backoff would redeliver an already-processed message immediately; it
        // returns on visibility expiry instead.
        await GuardAsync(
            () => DeleteHandledAsync(message, cancellationToken),
            envelope, message, cancellationToken).ConfigureAwait(false);
    }

    private async Task DeleteHandledAsync(Message message, CancellationToken cancellationToken)
    {
        try
        {
            await DeleteAsync(message, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
#pragma warning disable CA1031 // Re-thrown wrapped; GuardAsync reports it.
        catch (Exception error)
#pragma warning restore CA1031
        {
            throw new BabelQueueException(
                "The handler succeeded but the message could not be deleted from SQS; "
                + "it will be redelivered after its visibility timeout.", error);
        }
    }

    /// <summary>
    /// Runs a failure-path broker call (release / dead-letter / delete) so it can never stop
    /// the poll loop: a broker error (e.g. <c>ReceiptHandleIsInvalid</c> after the visibility
    /// window lapsed, throttling, a network fault) is reported via <c>OnError</c> and the
    /// message is left to SQS's visibility expiry; a cancellation on shutdown is swallowed.
    /// </summary>
    private async Task GuardAsync(
        Func<Task> operation, Envelope envelope, Message message, CancellationToken cancellationToken)
    {
        try
        {
            await operation().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutting down: the message returns on visibility expiry.
        }
#pragma warning disable CA1031 // The consume loop must survive any broker exception.
        catch (Exception error)
#pragma warning restore CA1031
        {
            _options.OnError?.Invoke(error, envelope, message);
        }
    }

    private async Task HandleUnknownUrnAsync(
        string urn, Envelope envelope, Message message, CancellationToken cancellationToken)
    {
        if (_options.OnUnknownUrn is not null)
        {
            await _options.OnUnknownUrn(envelope, message, cancellationToken).ConfigureAwait(false);
            await DeleteAsync(message, cancellationToken).ConfigureAwait(false);
            return;
        }

        switch (_unknownUrnStrategy)
        {
            case UnknownUrnStrategy.Delete:
                await DeleteAsync(message, cancellationToken).ConfigureAwait(false);
                break;
            case UnknownUrnStrategy.Release:
                await ReleaseAsync(message, _options.UnknownUrnReleaseDelaySeconds, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case UnknownUrnStrategy.DeadLetter:
                await DeadLetterAsync(envelope, cancellationToken).ConfigureAwait(false);
                await DeleteAsync(message, cancellationToken).ConfigureAwait(false);
                break;
            default:
                // fail (validated in the constructor): report, leave the message to SQS.
                _options.OnError?.Invoke(new UnknownUrnException(urn), envelope, message);
                break;
        }
    }

    /// <summary>
    /// Sends the envelope, annotated with a <c>dead_letter</c> block (reason
    /// <c>unknown_urn</c>), to the configured DLQ. No DLQ configured: no-op (the caller
    /// then deletes — the documented degrade-to-delete).
    /// </summary>
    private async Task DeadLetterAsync(Envelope envelope, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(_options.DeadLetterQueueUrl))
        {
            return;
        }

        var dead = DeadLetters.Annotate(envelope, "unknown_urn", envelope.Meta?.Queue ?? string.Empty);
        var request = new SendMessageRequest
        {
            QueueUrl = _options.DeadLetterQueueUrl,
            MessageBody = EnvelopeCodec.Encode(dead),
            MessageAttributes = SqsAttributes.Project(dead),
        };

        // A FIFO DLQ (<queue>.dlq.fifo, broker-bindings.md §3) requires a group id; dedupe on
        // meta.id, matching SqsPublisher.
        if (_options.DeadLetterQueueUrl.EndsWith(".fifo", StringComparison.Ordinal))
        {
            request.MessageGroupId = string.IsNullOrEmpty(envelope.Meta?.Queue)
                ? SqsPublisher.QueueName(_queueUrl)
                : envelope.Meta.Queue;
            if (!string.IsNullOrEmpty(envelope.Meta?.Id))
            {
                request.MessageDeduplicationId = envelope.Meta.Id;
            }
        }

        await _client.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases a received message for redelivery after <paramref name="delaySeconds"/>
    /// (clamped to 0–43200) via <c>ChangeMessageVisibility</c>; the message is not deleted.
    /// </summary>
    private async Task ReleaseAsync(Message message, int delaySeconds, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(message.ReceiptHandle))
        {
            return;
        }

        await _client.ChangeMessageVisibilityAsync(
            new ChangeMessageVisibilityRequest
            {
                QueueUrl = _queueUrl,
                ReceiptHandle = message.ReceiptHandle,
                VisibilityTimeout = Math.Clamp(delaySeconds, 0, MaxVisibilityTimeoutSeconds),
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The <c>bq-schema-version</c> attribute's string value exactly as sent, or <c>null</c> when absent.</summary>
    private static string? SchemaVersionValue(Message message)
        => message.MessageAttributes is not null
            && message.MessageAttributes.TryGetValue(SchemaVersionAttribute, out var attribute)
            ? attribute?.StringValue
            : null;

    /// <summary>
    /// The shared cross-SDK "blank" definition for <c>bq-schema-version</c>: the empty string, or a value made up
    /// <b>only</b> of ASCII whitespace (space, <c>\t</c>, <c>\n</c>, U+000B, <c>\f</c>, <c>\r</c>). Anything else
    /// (NBSP, U+001C–U+001F, U+0085, U+FEFF, …) is <b>not</b> blank, unlike <see cref="string.IsNullOrWhiteSpace"/>.
    /// </summary>
    private static bool IsBlankSchemaVersion(string value)
    {
        foreach (var c in value)
        {
            if (c != ' ' && (c < '\t' || c > '\r'))
            {
                return false;
            }
        }

        return true;
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
