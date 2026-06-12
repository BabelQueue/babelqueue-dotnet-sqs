using Amazon.SQS.Model;

namespace BabelQueue.Sqs;

/// <summary>
/// Processes one decoded, validated envelope and the raw SQS message it arrived on.
/// Completing normally acknowledges it (the consumer deletes it); throwing leaves it
/// for SQS to redeliver after the visibility timeout.
/// </summary>
public delegate Task BabelHandler(Envelope envelope, Message message, CancellationToken cancellationToken);
