using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
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
/// trimming — so <c>"2"</c>, <c>"x"</c>, <c>"01"</c> and <c>" 1"</c> are all unknown), the body is never decoded or routed
/// and <c>OnError</c> is notified (with an empty, undecoded envelope). It is settled on its <b>first delivery</b>
/// (ADR-0037 decision 1), whatever <c>ApproximateReceiveCount</c> says: under <c>UnknownUrnStrategy = delete</c> it is
/// deleted (dropped); with <see cref="SqsConsumerOptions.DeadLetterQueueUrl"/> set the body is written <b>exactly as
/// received</b> (no <c>dead_letter</c> block) to the DLQ and then deleted — a failed write leaves it undeleted and is
/// reported; with no DLQ it is left to the queue's native mechanism (not deleted, not released: visibility expiry
/// redelivers it and the redrive policy moves it to the native DLQ after <c>maxReceiveCount</c>). Only the explicitly
/// configured <see cref="SqsConsumerOptions.UnknownUrnStrategy"/> is read (default <c>fail</c>;
/// <c>release</c> behaves as <c>fail</c>); the <see cref="SqsConsumerOptions.OnUnknownUrn"/> hook never applies to it.
/// A missing or blank (empty or ASCII-whitespace-only: space, tab, LF, VT, FF, CR) attribute changes nothing, and a
/// <c>"1"</c> attribute still goes through the post-decode <see cref="EnvelopeCodec.Accepts"/> check.
/// </para>
/// <para>
/// A poison body (not a JSON object, no valid <c>job</c>/<c>urn</c> identity, or any other non-conformant envelope) is
/// never handed to a handler and never retried (ADR-0037 decision 2). It follows the same settle: <c>delete</c> drops it;
/// with a DLQ URL the ADR-0035 record (<c>reason: "poison"</c>) is written to the DLQ and the message deleted after the
/// write; with no DLQ it is left to the native mechanism.
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
    private readonly RetryBackoff _retryBackoff;

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
        _retryBackoff = new RetryBackoff(_options);
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
            // ADR-0037 decision 1: settled on the first delivery, never decoded, never retried. The body is deliberately
            // not decoded, so OnError gets an empty envelope.
            var rejected = new BabelQueueException(
                $"Rejected an SQS message: unsupported {SchemaVersionAttribute} attribute '{declaredVersion}' "
                + $"(supported: {EnvelopeCodec.SchemaVersion}); body not decoded.");
            var empty = new Envelope(null, null, null, null, 0, null);
            await SettleTerminalAsync(message, () => message.Body ?? string.Empty, empty, rejected, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        var envelope = Reconcile(
            EnvelopeCodec.Decode(message.Body ?? string.Empty),
            ReceiveCount(message));

        if (!EnvelopeCodec.Accepts(envelope))
        {
            // ADR-0037 decision 2: the poison route -- the ADR-0035 record to the DLQ, never a retry.
            var cause = new BabelQueueException("Rejected a non-conformant BabelQueue envelope from SQS.");
            await SettleTerminalAsync(
                message,
                () => Encoding.UTF8.GetString(PoisonBody(
                    Encoding.UTF8.GetBytes(message.Body ?? string.Empty),
                    cause.Message,
                    cause.GetType().FullName,
                    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    SqsPublisher.QueueName(_queueUrl),
                    envelope.Attempts)),
                envelope,
                cause,
                cancellationToken).ConfigureAwait(false);
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
                    () => ReleaseAsync(message, HandlerBackoffSeconds(message), cancellationToken),
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

    /// <summary>
    /// ADR-0037 decisions 1 and 2: settle a delivery that is terminal on its first delivery (a version-gated message or a
    /// poison body). Reads only the configured <c>UnknownUrnStrategy</c> (never the <c>OnUnknownUrn</c> hook):
    /// <c>delete</c> deletes it; with a DLQ URL the record from <paramref name="record"/> is written to the DLQ and the
    /// message deleted only after the write succeeded (a failed write leaves it undeleted: unsettled, reported); with no
    /// DLQ it is handed to the native mechanism -- not deleted, not released -- so visibility expiry redelivers it and the
    /// queue's <c>RedrivePolicy</c> applies. The rejection is reported after the settle.
    /// </summary>
    private async Task SettleTerminalAsync(
        Message message,
        Func<string> record,
        Envelope envelope,
        BabelQueueException rejected,
        CancellationToken cancellationToken)
    {
        if (_unknownUrnStrategy == UnknownUrnStrategy.Delete)
        {
            await GuardAsync(() => DeleteAsync(message, cancellationToken), envelope, message, cancellationToken)
                .ConfigureAwait(false);
        }
        else if (!string.IsNullOrEmpty(_options.DeadLetterQueueUrl))
        {
            await GuardAsync(
                async () =>
                {
                    await WriteDeadLetterRecordAsync(message, record(), cancellationToken).ConfigureAwait(false);
                    await DeleteAsync(message, cancellationToken).ConfigureAwait(false);
                },
                envelope,
                message,
                cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Dead-lettering off (or the dead_letter strategy with no DLQ URL): native. Neither deleted nor released.
            Trace.TraceWarning(
                "BabelQueue.Sqs: left a terminal message on its first delivery to the queue's native mechanism "
                + "(no DeadLetterQueueUrl): visibility expiry redelivers it and the RedrivePolicy applies.");
        }

        _options.OnError?.Invoke(rejected, envelope, message);
    }

    /// <summary>Writes <paramref name="body"/> as the body of a new message on the DLQ (FIFO: group = queue name, dedupe = SQS message id).</summary>
    private async Task WriteDeadLetterRecordAsync(Message message, string body, CancellationToken cancellationToken)
    {
        var request = new SendMessageRequest { QueueUrl = _options.DeadLetterQueueUrl, MessageBody = body };
        if (_options.DeadLetterQueueUrl!.EndsWith(".fifo", StringComparison.Ordinal))
        {
            request.MessageGroupId = SqsPublisher.QueueName(_queueUrl);
            request.MessageDeduplicationId = string.IsNullOrEmpty(message.MessageId) ? Guid.NewGuid().ToString("N") : message.MessageId;
        }

        await _client.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);
    }

    private static readonly JsonWriterOptions PoisonWriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Indented = false,
    };

    /// <summary>
    /// The poison DLQ record (ADR-0035, <see cref="PoisonRecord"/>): a body that is a strict JSON object without a
    /// top-level <c>dead_letter</c> key is spliced byte for byte; anything else is carried as <c>raw</c> (plain text when
    /// valid UTF-8, otherwise base64 plus <c>raw_encoding: "base64"</c>). The <c>dead_letter</c> block has reason
    /// <c>poison</c>.
    /// </summary>
    internal static byte[] PoisonBody(byte[] body, string? error, string? exception, long failedAt, string originalQueue, int attempts)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, PoisonWriterOptions))
        {
            writer.WriteStartObject();
            writer.WriteString("reason", "poison");
            if (error is null) { writer.WriteNull("error"); } else { writer.WriteString("error", error); }
            if (exception is null) { writer.WriteNull("exception"); } else { writer.WriteString("exception", exception); }
            writer.WriteNumber("failed_at", failedAt);
            writer.WriteString("original_queue", originalQueue);
            writer.WriteNumber("attempts", attempts);
            writer.WriteString("lang", EnvelopeCodec.SourceLang);
            writer.WriteEndObject();
        }

        return PoisonRecord.Build(body, Encoding.UTF8.GetString(stream.ToArray()));
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

        // A FIFO DLQ (<queue>-dlq.fifo, broker-bindings.md §3) requires a group id; dedupe on
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
    /// The ADR-0034 §9 backoff for a failed handler: <c>n = ApproximateReceiveCount</c> (= attempts + 1),
    /// rounded up to whole seconds (SQS visibility is integral) and clamped to the SQS range.
    /// </summary>
    private int HandlerBackoffSeconds(Message message)
    {
        var n = int.TryParse(ReceiveCount(message), NumberStyles.Integer, CultureInfo.InvariantCulture, out var count) && count > 1
            ? count
            : 1;
        var delay = Math.Ceiling(_retryBackoff.DelaySeconds(n));
        return (int)Math.Min(delay, MaxVisibilityTimeoutSeconds);
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
