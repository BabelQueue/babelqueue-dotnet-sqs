namespace BabelQueue.Sqs;

/// <summary>
/// The ADR-0034 §9 worker backoff, <c>d(n) = min(max, base * multiplier^(n-1))</c> seconds with optional
/// full jitter. This is a local copy of the formula in <c>BabelQueue.RetryBackoffPolicy</c> (core): this
/// package still references a published core that predates that type, so it cannot consume it yet.
/// Switch to the core helper once <c>BabelQueue.Core</c> ships it.
/// </summary>
internal sealed class RetryBackoff
{
    private readonly double _base;
    private readonly double _multiplier;
    private readonly double _max;
    private readonly bool _fullJitter;
    private readonly Func<double> _random;

    public RetryBackoff(SqsConsumerOptions options, Func<double>? random = null)
    {
        _base = options.RetryBackoff;
        _multiplier = options.RetryBackoffMultiplier;
        _max = options.RetryBackoffMax;
        if (double.IsNaN(_base) || double.IsInfinity(_base) || _base < 0)
        {
            throw new ArgumentException("RetryBackoff must be a finite number >= 0.", nameof(options));
        }
        if (double.IsNaN(_multiplier) || double.IsInfinity(_multiplier) || _multiplier <= 0)
        {
            throw new ArgumentException("RetryBackoffMultiplier must be a finite number > 0.", nameof(options));
        }
        if (double.IsNaN(_max) || double.IsInfinity(_max) || _max < 0)
        {
            throw new ArgumentException("RetryBackoffMax must be a finite number >= 0 (0 = no cap).", nameof(options));
        }
        _fullJitter = options.RetryJitter switch
        {
            "none" => false,
            "full" => true,
            _ => throw new ArgumentException(
                $"RetryJitter must be 'none' or 'full', got '{options.RetryJitter}'.", nameof(options)),
        };
        _random = random ?? (() => Random.Shared.NextDouble());
    }

    /// <summary>The delay in seconds before attempt <paramref name="n"/> (1-based, <c>attempts + 1</c>).</summary>
    public double DelaySeconds(int n)
    {
        if (_base == 0)
        {
            return 0;
        }

        var d = _base * Math.Pow(_multiplier, Math.Max(1, n) - 1);
        if (double.IsNaN(d) || double.IsInfinity(d))
        {
            d = double.MaxValue;
        }
        if (_max > 0)
        {
            d = Math.Min(_max, d);
        }
        return _fullJitter ? _random() * d : d;
    }
}
