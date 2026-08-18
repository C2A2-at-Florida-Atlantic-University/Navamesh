"""
calibration.py — raw ADC -> soil moisture band.

The field node (RAK4631) performs NO calibration. It transmits the raw averaged
ADC count and this module is the single place where that count becomes a
soil-moisture judgement, so calibration can be retuned by editing this table and
restarting the bridge — no reflashing of deployed nodes.

Hardware context:
    RAK4631 samples at 12-bit resolution against an AR_INTERNAL_3_0 reference,
    so volts ~= raw_adc * 3.0 / 4096. The reading saturates at 4095 for any
    input >= 3.0 V, which is why dry air and dry soil both read 4095.

WHY BANDS AND NOT A PERCENTAGE
------------------------------
Bench calibration 15-18 Aug 2026 (100 g soil samples, kneaded in sealed bags,
equilibrated overnight, 3-4 probe positions each) measured this response:

    moisture   mean ADC   sd    positions
      0%         4095      0    n=6
      5%         4091      -    day 1
      7%         4095      0    4095, 4095
      9.5%       4095      0    4095, 4095          <- still no signal
     10%         1603    480    980, 2052, 1900, 1482
     15%         1167    128    1060, 1133, 1308
     20%          322     89    380, 219, 366
     25%          280     92    223, 387, 231       <- indistinguishable from 20%
     30%          167     12    158, 175
     water         44      0    n=3

Three properties of that data rule out reporting a percentage:

1. NOTHING BELOW ~10%. Every sample from 0% to 9.5% sits on the 4095 rail.
   The sensor cannot distinguish bone-dry soil from soil at 9.5% moisture.

2. THE TRANSITION IS A STEP. Between 9.5% and 10% the reading falls ~4000
   counts — essentially the sensor's entire range inside half a percentage
   point. There is no gradient to interpolate across.

3. SATURATION ABOVE 20%. 20% (322) and 25% (280) differ by 42 counts with
   sd ~90 on each: statistically identical. Above 20% the curve is flat.

What remains is a usable middle where the reading genuinely tracks moisture,
bracketed by two regions where it does not. A percentage would imply
precision the hardware has not demonstrated; a band states what is known.
"""

from typing import Optional

# Band boundaries in raw ADC. Derived from the table above.
#
#   DRY_FLOOR_ADC   anything at or above this is on the 4095 rail: no water
#                   detected. Every measured sample from 0% to 9.5% is here.
#   WET_CEIL_ADC    below this the curve has gone flat (8 counts per percent
#                   between 20% and 25%): saturated, no further resolution.
#
# Every individual position measurement in the bench data classifies correctly
# under these two thresholds, with no overlap between bands.
DRY_FLOOR_ADC = 3900
WET_CEIL_ADC = 400

DRY = "DRY"     # <= ~9.5% moisture. Below detection. Needs water.
DAMP = "DAMP"   # ~10-19%. The resolvable range.
WET = "WET"     # >= 20%. At or past field capacity. Do not irrigate.

# Measured points for the coarse percentage estimate, ADC descending.
# Only spans the DAMP band — outside it the sensor has no resolution and
# adc_to_percent() deliberately returns None rather than inventing a number.
CURVE = [
    (1603, 10.0),
    (1167, 15.0),
    (322, 20.0),
]


def adc_to_band(raw_adc: float) -> str:
    """
    Classify a raw ADC count into DRY / DAMP / WET.

    This is the authoritative output of this module. It is what the bench data
    supports, and what downstream consumers should act on.

    A reading on the rail means no conductive path between the probe pins —
    which is true of bone-dry soil, of soil at 9.5% moisture, and of a probe
    sitting in an air gap. All three are reported DRY, because the sensor
    genuinely cannot tell them apart.
    """
    if raw_adc >= DRY_FLOOR_ADC:
        return DRY
    if raw_adc < WET_CEIL_ADC:
        return WET
    return DAMP


def adc_to_percent(raw_adc: float) -> Optional[float]:
    """
    Coarse moisture estimate, DAMP band only.

    Returns None outside the DAMP band — not because of an error, but because
    the sensor has no resolution there. Outside 400-3900 a number would be
    fabricated, and a caller that receives None can say "dry" or "saturated"
    instead of "0.0%" or "100.0%".

    Accuracy caveat even inside the band: the 10% and 15% samples differ by
    436 counts while the 10% sample's own position-to-position spread was 480.
    Those two levels do not separate cleanly, so treat this figure as +/- 5%
    and never as a control input. Use adc_to_band() for decisions.
    """
    if adc_to_band(raw_adc) != DAMP:
        return None

    if raw_adc >= CURVE[0][0]:
        return CURVE[0][1]
    if raw_adc <= CURVE[-1][0]:
        return CURVE[-1][1]

    for (a0, p0), (a1, p1) in zip(CURVE, CURVE[1:]):
        if a1 <= raw_adc <= a0:
            return round(p0 + (p1 - p0) * (a0 - raw_adc) / (a0 - a1), 1)

    return None  # unreachable given the bounds checks above
