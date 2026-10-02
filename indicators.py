"""
Pure-Python indicator math.
No numpy — keeps memory low for the multi-SSID Railway instances.
"""

def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        delta = closes[i] - closes[i - 1]
        if delta >= 0:
            gains += delta
        else:
            losses += -delta
    avg_gain = gains / period
    avg_loss = losses / period
    for i in range(period + 1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gain = delta if delta > 0 else 0.0
        loss = -delta if delta < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def compute_bollinger(closes, period=20, mult=2.0):
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((x - middle) ** 2 for x in window) / period
    stddev = variance ** 0.5
    return middle + mult * stddev, middle, middle - mult * stddev


def compute_ema(closes, period=6):
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def recompute(closes, rsi_p=14, bb_p=20, bb_std=2.0, ema_p=6):
    """
    Returns a dict with all indicator values for the latest close.
    BB % uses the full band width (upper - lower) as the denominator,
    so pct_to_upper + pct_to_lower = 100.
    """
    rsi = compute_rsi(closes, rsi_p)
    bb_u, bb_m, bb_l = compute_bollinger(closes, bb_p, bb_std)
    ema = compute_ema(closes, ema_p)
    price = closes[-1] if closes else None

    bb_bandwidth = None
    pct_to_upper = None
    pct_to_lower = None
    if bb_u is not None and bb_l is not None and bb_m and bb_m > 0 and price is not None:
        bb_bandwidth = (bb_u - bb_l) / bb_m * 100.0
        width = bb_u - bb_l
        if width > 0:
            pct_to_upper = max(0.0, min(100.0, (bb_u - price) / width * 100.0))
            pct_to_lower = max(0.0, min(100.0, (price - bb_l) / width * 100.0))

    ema_signal = None
    if ema is not None and price is not None:
        ema_signal = "ABOVE" if price > ema else "BELOW"

    return {
        "rsi": rsi,
        "bb_upper": bb_u,
        "bb_middle": bb_m,
        "bb_lower": bb_l,
        "bb_bandwidth": bb_bandwidth,
        "bb_pct_to_upper": pct_to_upper,
        "bb_pct_to_lower": pct_to_lower,
        "ema": ema,
        "ema_signal": ema_signal,
    }
