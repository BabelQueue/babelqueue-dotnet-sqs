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
    /// When set, it takes precedence over <see cref="UnknownUrnStrategy"/>.
    /// </summary>
    public Func<Envelope, Message, CancellationToken, Task>? OnUnknownUrn { get; set; }

    /// <summary>
    /// Backoff (seconds) applied when a handler throws: the message is released with
    /// <c>ChangeMessageVisibility(VisibilityTimeout = backoff)</c> and is NOT deleted
    /// (broker-bindings.md §3.5) — <c>0</c> (the default) redelivers it now, <c>N</c> after
    /// N seconds. Clamped to SQS's 0–43200 range. <c>ApproximateReceiveCount</c> stays the
    /// attempt counter. <b>Poison-loop risk:</b> with the default <c>0</c> a message whose
    /// handler always throws is redelivered immediately, forever — configure a native
    /// redrive policy (<c>maxReceiveCount</c> → <c>&lt;queue&gt;.dlq</c>) to bound retries.
    /// </summary>
    public int RetryBackoffSeconds { get; set; }

    /// <summary>
    /// What to do with a message whose URN has no handler (and no <see cref="OnUnknownUrn"/>):
    /// one of the <see cref="BabelQueue.UnknownUrnStrategy"/> constants —
    /// <c>fail</c> (default: report <see cref="UnknownUrnException"/> via <see cref="OnError"/>,
    /// leave the message), <c>delete</c>, <c>release</c> (<c>ChangeMessageVisibility</c> with
    /// <see cref="UnknownUrnReleaseDelaySeconds"/>) or <c>dead_letter</c> (send to
    /// <see cref="DeadLetterQueueUrl"/> with a <c>dead_letter</c> block, then delete; degrades
    /// to delete when no DLQ URL is configured). Any other value is rejected by the
    /// <see cref="SqsConsumer"/> constructor with an <see cref="ArgumentException"/>; the value
    /// is read once, at construction.
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
    /// strategy; <c>null</c> disables the DLQ (the strategy then degrades to delete).
    /// </summary>
    public string? DeadLetterQueueUrl { get; set; }
}
