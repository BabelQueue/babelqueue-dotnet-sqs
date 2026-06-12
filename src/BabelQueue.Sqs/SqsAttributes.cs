using System.Globalization;
using Amazon.SQS.Model;

namespace BabelQueue.Sqs;

/// <summary>
/// Projects the envelope's contract fields onto native SQS <c>MessageAttributes</c> —
/// a redundant, routable view of the body (the body stays authoritative). Contract §3.2.
/// </summary>
internal static class SqsAttributes
{
    public static Dictionary<string, MessageAttributeValue> Project(Envelope envelope)
    {
        var attributes = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        AddString(attributes, "bq-job", envelope.Job);
        AddString(attributes, "bq-trace-id", envelope.TraceId);

        var meta = envelope.Meta;
        if (meta is not null)
        {
            AddString(attributes, "bq-message-id", meta.Id);
            attributes["bq-schema-version"] = Number(meta.SchemaVersion.ToString(CultureInfo.InvariantCulture));
            AddString(attributes, "bq-source-lang", meta.Lang);
            attributes["bq-created-at"] = Number(meta.CreatedAt.ToString(CultureInfo.InvariantCulture));
        }

        return attributes;
    }

    private static void AddString(Dictionary<string, MessageAttributeValue> attributes, string key, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            attributes[key] = new MessageAttributeValue { DataType = "String", StringValue = value };
        }
    }

    private static MessageAttributeValue Number(string value)
        => new() { DataType = "Number", StringValue = value };
}
