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
    /// <see cref="OnUnknownUrn"/>), or a throwing handler. The poll loop never stops.
    /// </summary>
    public Action<Exception, Envelope, Message>? OnError { get; set; }

    /// <summary>Called instead of erroring when a URN has no handler; the message is then deleted.</summary>
    public Func<Envelope, Message, CancellationToken, Task>? OnUnknownUrn { get; set; }
}
