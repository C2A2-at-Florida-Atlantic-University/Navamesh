"""
calibration.py — raw ADC -> soil moisture percentage.

The field node (RAK4631) performs NO calibration. It transmits the raw averaged
ADC count and this module is the single place where that count becomes a
percentage, so calibration can be retuned by editing .env and restarting the
bridge — no reflashing of deployed nodes.

Hardware context for anyone refitting the curve:
    RAK4631 samples at 12-bit resolution against an AR_INTERNAL_3_0 reference,
    so volts ~= raw_adc * 3.0 / 4096. The reading saturates at 4095 for any
    input >= 3.0 V, which is why dry air reads 4095.

Reference points measured on an HD-38 (see the firmware README):
    dry air 4095 | dry soil ~3120 | moist soil ~2879 | wet/muddy 1567 | water 849
"""

from typing import Optional


def adc_to_percent(raw_adc: float, dry: int, wet: int) -> Optional[float]:
    """
    Convert a raw ADC count to a soil moisture percentage.

    Linear interpolation between two calibration points: `dry` reads as 0% and
    `wet` reads as 100%. On an HD-38 the ADC falls as moisture rises, so `dry`
    is normally the larger number — but the maths does not require that, so an
    inverted sensor works too.

    Only the DERIVED percentage is clamped to 0-100. The raw ADC passed in is
    never modified, and callers must store it verbatim.

    Returns None if the calibration span is degenerate (dry == wet), rather
    than raising, so one bad config value cannot take down the bridge.
    """
    span = dry - wet
    if span == 0:
        return None

    percent = (dry - raw_adc) / span * 100.0
    # Clamp the RESULT ONLY. The raw ADC is untouched.
    percent = max(0.0, min(100.0, percent))
    return round(percent, 2)
