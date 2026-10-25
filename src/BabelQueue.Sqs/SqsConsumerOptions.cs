using Amazon.SQS.Model;

namespace BabelQueue.Sqs;

/// <summary>Tuning and hooks for <see cref="SqsConsumer"/>.</summary>
public sealed class SqsConsumerOptions
{
    /// <summary>Long-poll wait seconds (0–20, default 20).</summary>
    public int WaitTimeSeconds { get; set; } = 20;

    /// <summary>Reservation window applied on receive (seconds); <c>null</c> leaves the queue default.</summary>
    public int? VisibilityTimeout { get; set; }

    /// <summary>Max messages per receive (default 10).</summary>
    public int MaxMessages { get; set; } = 10;

    /// <summary>
    /// Called for a non-conformant envelope, an unmapped URN (with no
    /// <see cref="OnUnknownUrn"/>), a throwing handler, or a failing failure-path broker call.
    /// A delete that fails after a successful handler arrives as a <see cref="BabelQueueException"/>
    /// wrapping the broker error (the message is not released). The poll loop never stops.
    /// </summary>
    public Action<Exception, Envelope, Message>? OnError { get; set; }

    /// <summary>
    /// Called instead of erroring when a URN has no handler; the message is then deleted.
    /// When set, it takes precedence over <see cref="UnknownUrnStrategy"/> for an unknown URN <b>only</b>: it never
    /// affects a schema-version-gated message or a poison body (ADR-0037), which read the configured
    /// <see cref="UnknownUrnStrategy"/> alone (default <c>fail</c>).
    /// </summary>
    public Func<Envelope, Message, CancellationToken, Task>? OnUnknownUrn { get; set; }

    /// <summary>
    /// Base backoff (seconds) applied when a handler throws: the message is released with
    /// <c>ChangeMessageVisibility(VisibilityTimeout = backoff)</c> and is NOT deleted
    /// (broker-bindings.md §3.5) — <c>0</c> (the default) redelivers it now, <c>N</c> after
    /// N seconds. Together with <see cref="RetryBackoffMultiplier"/>, <see cref="RetryBackoffMax"/> and
    /// <see cref="RetryJitter"/> it forms the ADR-0034 §9 formula
    /// <c>d(n) = min(max, base * multiplier^(n-1))</c>, <c>n = ApproximateReceiveCount</c>
    /// (= <c>attempts + 1</c>); with the defaults it is the constant delay it always was.
    /// The result is clamped to SQS's 0–43200 range (<c>RetryBackoffMax = 0</c> means no cap of its
    /// own — the SQS 12 h ceiling still applies). <c>ApproximateReceiveCount</c> stays the
    /// attempt counter. <b>Poison-loop risk:</b> with the default <c>0</c> a message whose
    /// handler always throws is redelivered immediately, forever — configure a native
    /// redrive policy (<c>maxReceiveCount</c> → <c>&lt;queue&gt;.dlq</c>) to bound retries.
    /// Validated, together with the three options below, by the <see cref="SqsConsumer"/> constructor.
    /// </summary>
    public double RetryBackoff { get; set; }

    /// <summary>Growth factor per attempt (default <c>1</c> = constant; must be &gt; 0). ADR-0034 §9.</summary>
    public double RetryBackoffMultiplier { get; set; } = 1;

    /// <summary>Cap in seconds on the computed backoff (default <c>0</c> = no cap). ADR-0034 §9.</summary>
    public double RetryBackoffMax { get; set; }

    /// <summary>
    /// <c>"none"</c> (default: the exact delay) or <c>"full"</c> (<c>uniform(0, d(n))</c>). ADR-0034 §9.
    /// Any other value is rejected by the <see cref="SqsConsumer"/> constructor.
    /// </summary>
    public string RetryJitter { get; set; } = "none";

    /// <summary>
    /// Alias of <see cref="RetryBackoff"/> kept for compatibility (whole seconds; a negative value
    /// reads as <c>0</c>, as it always was clamped to the SQS range).
    /// </summary>
    public int RetryBackoffSeconds
    {
        get => (int)Math.Min(int.MaxValue, Math.Ceiling(RetryBackoff));
        set => RetryBackoff = Math.Max(0, value);
    }

    /// <summary>
    /// What to do with a message whose URN has no handler (and no <see cref="OnUnknownUrn"/>):
    /// one of the <see cref="BabelQueue.UnknownUrnStrategy"/> constants —
    /// <c>fail</c> (default: report <see cref="UnknownUrnException"/> via <see cref="OnError"/>,
    /// leave the message), <c>delete</c>, <c>release</c> (<c>ChangeMessageVisibility</c> with
    /// <see cref="UnknownUrnReleaseDelaySeconds"/>) or <c>dead_letter</c> (send to
    /// <see cref="DeadLetterQueueUrl"/> with a <c>dead_letter</c> block, then delete; degrades
    /// to delete when no DLQ URL is configured). Any other value is rejected by the
    /// <see cref="SqsConsumer"/> constructor with an <see cref="ArgumentException"/>; the value
    /// is read once, at construction. The configured value also decides a schema-version-gated message and a poison
    /// body (ADR-0037): <c>delete</c> deletes them; otherwise they are written to <see cref="DeadLetterQueueUrl"/>
    /// (raw body for a gated message, the ADR-0035 record for a poison body) and deleted, or, with no DLQ URL,
    /// left to the queue's native <c>RedrivePolicy</c> (<c>release</c> behaves as <c>fail</c>).
    /// </summary>
    public string UnknownUrnStrategy { get; set; } = BabelQueue.UnknownUrnStrategy.Fail;

    /// <summary>
    /// Visibility delay (seconds) for the <c>release</c> unknown-URN strategy (default <c>0</c> =
    /// redeliver now; clamped to 0–43200). Without a native redrive policy a released message
    /// loops until a consumer that knows the URN picks it up.
    /// </summary>
    public int UnknownUrnReleaseDelaySeconds { get; set; }

    /// <summary>
    /// URL of the <c>&lt;queue&gt;.dlq</c> (or FIFO <c>&lt;queue&gt;.dlq.fifo</c>, sent with
    /// <c>MessageGroupId = meta.queue</c> and <c>MessageDeduplicationId = meta.id</c>) queue used by the <c>dead_letter</c> unknown-URN
    /// strategy, and the target of a poison record and of a raw schema-version-gated body (ADR-0037); <c>null</c>
    /// disables the DLQ (the unknown-URN strategy then degrades to delete; a poison or gated message is left to the
    /// native mechanism).
    /// Poison and schema-version-gated records sent to a FIFO DLQ carry <c>MessageGroupId</c> = the queue name and
    /// <c>MessageDeduplicationId</c> = the SQS message id (not <c>meta.queue</c> / <c>meta.id</c>: there is no usable envelope).
    /// </summary>
    public string? DeadLetterQueueUrl { get; set; }
}
