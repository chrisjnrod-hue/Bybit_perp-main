# sr_calculator.py - Support/Resistance level calculation
# Hybrid method: Woodie Pivots + Swing detection
import math
from typing import Dict, List, Any, Optional
from .logger import get_logger
from .config import SR_METHOD, SWING_LOOKBACK

logger = get_logger("sr_calculator")


class SRCalculator:
    """Calculate support/resistance levels per signal using hybrid method."""

    @staticmethod
    def calculate_woodie_pivots(high: float, low: float, close: float) -> Dict[str, float]:
        """
        Classic Woodie Pivot Points calculation.
        
        P  = (H + L + 2C) / 4
        R1 = (2 × P) − L
        S1 = (2 × P) − H
        R2 = P + (H − L)
        S2 = P − (H − L)
        """
        try:
            p = (high + low + 2 * close) / 4.0
            r1 = (2 * p) - low
            s1 = (2 * p) - high
            r2 = p + (high - low)
            s2 = p - (high - low)
            
            return {
                "pivot": p,
                "r1": r1,
                "s1": s1,
                "r2": r2,
                "s2": s2,
                "range": high - low,
            }
        except Exception as e:
            logger.debug("Woodie pivot calc failed: %s", e)
            return {}

    @staticmethod
    def calculate_fibonacci_pivots(high: float, low: float, close: float) -> Dict[str, float]:
        """
        Fibonacci Pivot Points.
        
        P  = (H + L + C) / 3
        R1 = P + 0.382 × (H − L)
        R2 = P + 0.618 × (H − L)
        R3 = P + 1.000 × (H − L)
        S1 = P − 0.382 × (H − L)
        S2 = P − 0.618 × (H − L)
        S3 = P − 1.000 × (H − L)
        """
        try:
            p = (high + low + close) / 3.0
            diff = high - low
            r1 = p + 0.382 * diff
            r2 = p + 0.618 * diff
            r3 = p + 1.000 * diff
            s1 = p - 0.382 * diff
            s2 = p - 0.618 * diff
            s3 = p - 1.000 * diff
            
            return {
                "pivot": p,
                "r1": r1,
                "r2": r2,
                "r3": r3,
                "s1": s1,
                "s2": s2,
                "s3": s3,
                "range": diff,
            }
        except Exception as e:
            logger.debug("Fibonacci pivot calc failed: %s", e)
            return {}

    @staticmethod
    def detect_swing_levels(closes: List[float], lookback: int = 20) -> Dict[str, Any]:
        """
        Detect recent swing highs and lows over lookback window.
        Returns dict with:
          - swing_highs: list of (index, price) tuples
          - swing_lows: list of (index, price) tuples
          - nearest_high: price of nearest swing high
          - nearest_low: price of nearest swing low
        """
        try:
            if len(closes) < lookback:
                window = closes
            else:
                window = closes[-lookback:]
            
            if len(window) < 3:
                return {"swing_highs": [], "swing_lows": [], "nearest_high": None, "nearest_low": None}
            
            swing_highs = []
            swing_lows = []
            
            # Simple: detect local max/min with window=2 around each point
            for i in range(1, len(window) - 1):
                if window[i] > window[i-1] and window[i] > window[i+1]:
                    swing_highs.append((i, window[i]))
                elif window[i] < window[i-1] and window[i] < window[i+1]:
                    swing_lows.append((i, window[i]))
            
            nearest_high = swing_highs[-1][1] if swing_highs else None
            nearest_low = swing_lows[-1][1] if swing_lows else None
            
            return {
                "swing_highs": swing_highs,
                "swing_lows": swing_lows,
                "nearest_high": nearest_high,
                "nearest_low": nearest_low,
            }
        except Exception as e:
            logger.debug("Swing detection failed: %s", e)
            return {"swing_highs": [], "swing_lows": [], "nearest_high": None, "nearest_low": None}

    @staticmethod
    def calculate_sr_for_signal(
        symbol: str,
        price: float,
        klines: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Calculate S/R levels for a signal using hybrid method.
        
        Returns dict with:
          - method: "hybrid", "woodie", "swing", or "fibonacci"
          - woodie: Woodie pivot dict
          - swing: Swing detection dict
          - score_contribution: 0.0-1.0 based on proximity to levels
          - nearest_resistance: price
          - nearest_support: price
          - description: human readable string
        """
        try:
            if not klines or len(klines) < 3:
                return {
                    "method": "none",
                    "score_contribution": 0.0,
                    "nearest_resistance": None,
                    "nearest_support": None,
                    "description": "Insufficient klines",
                }
            
            # Extract OHLC from klines (use available fields)
            closes = []
            highs = []
            lows = []
            
            for k in klines:
                try:
                    c = k.get("close")
                    if c is not None:
                        closes.append(float(c))
                    h = k.get("high") or k.get("close")
                    if h is not None:
                        highs.append(float(h))
                    l = k.get("low") or k.get("close")
                    if l is not None:
                        lows.append(float(l))
                except Exception:
                    continue
            
            if not closes or not highs or not lows:
                return {
                    "method": "none",
                    "score_contribution": 0.0,
                    "nearest_resistance": None,
                    "nearest_support": None,
                    "description": "No valid OHLC data",
                }
            
            # Use previous candle for pivot calculation
            prev_high = highs[-2] if len(highs) >= 2 else highs[-1]
            prev_low = lows[-2] if len(lows) >= 2 else lows[-1]
            prev_close = closes[-2] if len(closes) >= 2 else closes[-1]
            
            result: Dict[str, Any] = {
                "method": SR_METHOD,
                "score_contribution": 0.0,
                "nearest_resistance": None,
                "nearest_support": None,
                "description": "",
                "woodie": {},
                "swing": {},
            }
            
            if SR_METHOD in ("hybrid", "woodie", "fibonacci"):
                # Calculate Woodie pivots (always included in hybrid)
                if SR_METHOD in ("hybrid", "woodie"):
                    woodie = SRCalculator.calculate_woodie_pivots(prev_high, prev_low, prev_close)
                    result["woodie"] = woodie
                    if woodie:
                        result["nearest_resistance"] = woodie.get("r1")
                        result["nearest_support"] = woodie.get("s1")
                
                elif SR_METHOD == "fibonacci":
                    fib = SRCalculator.calculate_fibonacci_pivots(prev_high, prev_low, prev_close)
                    result["woodie"] = fib  # Store as woodie for consistency
                    if fib:
                        result["nearest_resistance"] = fib.get("r1")
                        result["nearest_support"] = fib.get("s1")
            
            if SR_METHOD in ("hybrid", "swing"):
                # Swing detection
                swing = SRCalculator.detect_swing_levels(closes, lookback=SWING_LOOKBACK)
                result["swing"] = swing
                if swing.get("nearest_high"):
                    if result["nearest_resistance"] is None:
                        result["nearest_resistance"] = swing["nearest_high"]
                if swing.get("nearest_low"):
                    if result["nearest_support"] is None:
                        result["nearest_support"] = swing["nearest_low"]
            
            # Calculate score contribution based on proximity to levels
            score = SRCalculator._score_proximity(
                price,
                result.get("nearest_resistance"),
                result.get("nearest_support"),
            )
            result["score_contribution"] = score
            
            # Build description
            desc_parts = []
            if result.get("nearest_resistance"):
                dist_r = abs(price - result["nearest_resistance"]) / price * 100
                desc_parts.append(f"R: {result['nearest_resistance']:.8f} ({dist_r:.2f}% away)")
            if result.get("nearest_support"):
                dist_s = abs(price - result["nearest_support"]) / price * 100
                desc_parts.append(f"S: {result['nearest_support']:.8f} ({dist_s:.2f}% away)")
            
            result["description"] = " | ".join(desc_parts) if desc_parts else "No nearby levels"
            
            return result
        
        except Exception as e:
            logger.exception("S/R calculation failed for %s", symbol)
            return {
                "method": "error",
                "score_contribution": 0.0,
                "nearest_resistance": None,
                "nearest_support": None,
                "description": str(e)[:100],
            }

    @staticmethod
    def _score_proximity(current_price: float, resistance: Optional[float], support: Optional[float]) -> float:
        """
        Score proximity to S/R levels (0.0 = far, 1.0 = very close).
        Closer to level = higher score.
        """
        try:
            scores = []
            
            if resistance is not None and resistance > 0:
                dist_to_r = abs(current_price - resistance) / resistance
                # Within 2% of resistance = +0.3, within 5% = +0.2, etc.
                if dist_to_r < 0.01:
                    scores.append(0.3)
                elif dist_to_r < 0.02:
                    scores.append(0.25)
                elif dist_to_r < 0.05:
                    scores.append(0.15)
                elif dist_to_r < 0.10:
                    scores.append(0.05)
            
            if support is not None and support > 0:
                dist_to_s = abs(current_price - support) / support
                # Within 2% of support = +0.3, within 5% = +0.2, etc.
                if dist_to_s < 0.01:
                    scores.append(0.3)
                elif dist_to_s < 0.02:
                    scores.append(0.25)
                elif dist_to_s < 0.05:
                    scores.append(0.15)
                elif dist_to_s < 0.10:
                    scores.append(0.05)
            
            return min(sum(scores), 1.0)  # Cap at 1.0
        except Exception:
            return 0.0
