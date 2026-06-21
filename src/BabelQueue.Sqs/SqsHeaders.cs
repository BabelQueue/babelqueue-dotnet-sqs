using Amazon.SQS.Model;

namespace BabelQueue.Sqs;

/// <summary>
/// Out-of-band transport-header carrier for SQS (ADR-0028) — the SQS counterpart of the Go
/// <c>mergeAttributes</c>/<c>headersFromAttributes</c> pair and the .NET core's
/// <see cref="BabelQueue.Tracing.Traceparent"/> seam.
/// </summary>
/// <remarks>
/// <para>
/// A W3C <c>traceparent</c> (and <c>tracestate</c>) for cross-hop span linkage rides as a
/// <b>String</b> <c>MessageAttribute</c> <b>beside</b> the frozen envelope — never inside it
/// (GR-1) — alongside the §3 <c>bq-*</c> contract projection. On the consume side the inbound
/// <c>MessageAttributes</c> are surfaced as a flat <see cref="Dictionary{TKey,TValue}"/> a handler
/// hands to <c>Telemetry.Wrap(handler, headers)</c>, so the consumer span becomes a true child of
/// the producer span. When no <c>traceparent</c> attribute is present the core falls back to the
/// v0.1 <c>trace_id</c> mapping (no regression).
/// </para>
/// <para>
/// SQS caps a message at 10 user <c>MessageAttributes</c>; the contract <c>bq-*</c> projection
/// (up to 6) plus <c>traceparent</c>(+<c>tracestate</c>) stays well under it. <see cref="Merge"/>
/// enforces the limit so unbounded extra headers can never push a send past it (SQS would reject
/// the whole message otherwise) and never clobbers a contract <c>bq-*</c> attribute.
/// </para>
/// </remarks>
public static class SqsHeaders
{
    /// <summary>The SQS per-message cap on user <c>MessageAttributes</c>.</summary>
    internal const int MaxMessageAttributes = 10;

    /// <summary>
    /// Overlays the out-of-band string <paramref name="headers"/> onto the contract attribute
    /// projection <paramref name="baseAttributes"/> as <c>String</c> <c>MessageAttributes</c>,
    /// without overwriting an existing <c>bq-*</c> attribute (the contract wins a key collision)
    /// and skipping empty keys/values. It stops once the message reaches the 10-attribute SQS limit
    /// so unbounded extra headers can never make SQS reject the send (the contract attributes are
    /// always preserved first). Keys are merged in ordinal order so the bounded subset is
    /// deterministic. A <c>null</c>/empty header map returns <paramref name="baseAttributes"/>
    /// unchanged, so a header-less publish is byte-identical to before.
    /// </summary>
    internal static Dictionary<string, MessageAttributeValue> Merge(
        Dictionary<string, MessageAttributeValue> baseAttributes,
        IReadOnlyDictionary<string, string>? headers)
    {
        ArgumentNullException.ThrowIfNull(baseAttributes);
        if (headers is null || headers.Count == 0)
        {
            return baseAttributes;
        }

        foreach (var key in headers.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            var value = headers[key];
            if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(value))
            {
                continue;
            }

            if (baseAttributes.ContainsKey(key))
            {
                continue; // never clobber a contract bq-* attribute
            }

            if (baseAttributes.Count >= MaxMessageAttributes)
            {
                break; // respect the SQS 10-attribute ceiling
            }

            baseAttributes[key] = new MessageAttributeValue { DataType = "String", StringValue = value };
        }

        return baseAttributes;
    }

    /// <summary>
    /// Maps inbound SQS <c>MessageAttributes</c> to a flat <see cref="Dictionary{TKey,TValue}"/>
    /// (the consume-side counterpart of <see cref="Merge"/>), reading each attribute's
    /// <c>StringValue</c>. Both the contract <c>bq-*</c> attributes and any out-of-band header
    /// (e.g. <c>traceparent</c>) surface — the core's tracing reads only the keys it knows. Empty
    /// values are skipped; an empty/<c>null</c> input yields an empty map, so a header-less delivery
    /// produces no usable parent and the consumer falls back to the v0.1 <c>trace_id</c> mapping.
    /// </summary>
    public static Dictionary<string, string> Extract(IDictionary<string, MessageAttributeValue>? attributes)
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        if (attributes is null)
        {
            return headers;
        }

        foreach (var (key, value) in attributes)
        {
            if (!string.IsNullOrEmpty(value?.StringValue))
            {
                headers[key] = value.StringValue;
            }
        }

        return headers;
    }
}
