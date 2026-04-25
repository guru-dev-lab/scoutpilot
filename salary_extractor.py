"""
Salary extraction from job descriptions.

Two-stage approach:
  1. Regex — fast, zero-cost pattern matching for common salary formats
  2. AI fallback — Haiku call when regex finds nothing but pay keywords are present

Returns: {"min": int, "max": int, "period": "yearly"|"hourly"|"monthly", "raw": str}
All values normalized to integers. Period preserved so the frontend can display correctly.
"""
import re
import json
import logging
from typing import Optional

logger = logging.getLogger("scoutpilot.salary")

# ── Regex patterns ──────────────────────────────────────────────────────────
# Matches formats like:
#   $60,000 - $80,000       $60k-$80k       $60K - $80K
#   $30/hr - $45/hr         $30 - $45 per hour
#   $120,000/year           $5,000/month
#   £45,000 - £55,000       €50,000 - €70,000  CA$80,000
#   USD 60,000              60,000 USD
#   $150k+  (single value)  $120,000 annually

_CURRENCY = r"(?:[$£€]|(?:USD|CAD|GBP|EUR|CA\$|AU\$)\s*)"
_NUMBER = r"(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?[kK]?|\d+(?:\.\d{1,2})?[kK]?)"
_RANGE_SEP = r"\s*[-–—to]+\s*"
_PERIOD_SUFFIX = r"(?:\s*(?:per|\/|a)\s*(?:year|yr|annum|annual|annually|hour|hr|hourly|month|mo|monthly|week|wk|weekly))?\.?"
# Allow optional period/unit after the first number in a range (e.g. "$25/hr - $35/hr")
_MID_PERIOD = r"(?:\s*(?:per|\/|a)\s*(?:year|yr|annum|annual|annually|hour|hr|hourly|month|mo|monthly|week|wk|weekly))?"

# Pattern 1: Currency + number + optional mid-period + range sep + number + optional period
_PAT_RANGE = re.compile(
    _CURRENCY + r"\s*" + _NUMBER + _MID_PERIOD + _RANGE_SEP + _CURRENCY + r"?\s*" + _NUMBER + _PERIOD_SUFFIX,
    re.IGNORECASE,
)

# Pattern 2: Single value with currency — "$120,000/year" or "$45/hr" or "$80k+"
_PAT_SINGLE = re.compile(
    _CURRENCY + r"\s*" + _NUMBER + r"\+?" + _PERIOD_SUFFIX,
    re.IGNORECASE,
)

# Pattern 3: "salary range: 60000 - 80000" or "compensation: 60k - 80k" (no currency symbol)
_PAT_LABELED = re.compile(
    r"(?:salary|compensation|pay|wage|earning|income|base)\s*(?:range|:)?\s*(?:" + _CURRENCY + r")?\s*"
    + _NUMBER + _MID_PERIOD + _RANGE_SEP + r"(?:" + _CURRENCY + r")?\s*" + _NUMBER + _PERIOD_SUFFIX,
    re.IGNORECASE,
)

# Period detection patterns
_HOURLY_PAT = re.compile(r"(?:per|\/|a)\s*(?:hour|hr|hourly)|\/hr|\/hour", re.IGNORECASE)
_MONTHLY_PAT = re.compile(r"(?:per|\/|a)\s*(?:month|mo|monthly)|\/mo|\/month", re.IGNORECASE)
_WEEKLY_PAT = re.compile(r"(?:per|\/|a)\s*(?:week|wk|weekly)|\/wk|\/week", re.IGNORECASE)

# Keywords that suggest pay info exists in the text (for AI fallback trigger)
_PAY_KEYWORDS = re.compile(
    r"\b(?:salary|compensation|pay\s*(?:range|rate|scale)|wage|remuneration|"
    r"stipend|earning|OTE|on[\s-]?target|base\s*(?:pay|salary)|"
    r"total\s*comp|hourly\s*rate|annual(?:ly)?|per\s*(?:year|hour|annum))\b",
    re.IGNORECASE,
)

# Negative patterns — contexts where dollar amounts aren't salary
_NEGATIVE_CONTEXT = re.compile(
    r"(?:revenue|funding|raised|valuation|market\s*cap|billion|trillion|"
    r"customer|client|user|account|saving|discount|benefit\s*(?:value|worth)|"
    r"insurance|401k|bonus|equity|stock|RSU|sign[\s-]?on|relocation)",
    re.IGNORECASE,
)


def _parse_number(s: str) -> float:
    """Parse a number string like '60,000', '60k', '60.5K', '120000' into a float."""
    s = s.strip().replace(",", "")
    multiplier = 1
    if s.lower().endswith("k"):
        s = s[:-1]
        multiplier = 1000
    return float(s) * multiplier


def _detect_period(match_text: str) -> str:
    """Detect if the matched salary text is hourly, monthly, weekly, or yearly."""
    if _HOURLY_PAT.search(match_text):
        return "hourly"
    if _MONTHLY_PAT.search(match_text):
        return "monthly"
    if _WEEKLY_PAT.search(match_text):
        return "weekly"
    return "yearly"


def _is_reasonable_salary(val_min: float, val_max: float, period: str) -> bool:
    """Sanity check: is this a plausible salary?"""
    if val_min < 0 or val_max < 0:
        return False
    if val_min > val_max and val_max > 0:
        return False

    if period == "yearly":
        # $15,000 - $500,000 yearly range is reasonable
        return 15000 <= max(val_min, val_max) <= 500000
    elif period == "hourly":
        # $8 - $250/hr is reasonable
        return 8 <= max(val_min, val_max) <= 300
    elif period == "monthly":
        # $1,000 - $40,000/month is reasonable
        return 1000 <= max(val_min, val_max) <= 50000
    elif period == "weekly":
        return 200 <= max(val_min, val_max) <= 12000
    return True


def _infer_period_from_value(val: float) -> str:
    """When no explicit period marker, guess from the magnitude."""
    if val <= 0:
        return "yearly"
    if val < 500:
        return "hourly"   # $30, $45 → likely hourly
    if val < 15000:
        return "monthly"  # $5,000 → likely monthly (or could be biweekly)
    return "yearly"       # $60,000+ → yearly


def extract_salary_regex(text: str) -> Optional[dict]:
    """
    Extract salary from text using regex.
    Returns {"min": int, "max": int, "period": str, "raw": str} or None.
    """
    if not text:
        return None

    # Try labeled patterns first (most reliable — "salary: X - Y")
    for pattern in [_PAT_LABELED, _PAT_RANGE]:
        for match in pattern.finditer(text):
            raw = match.group(0)

            # Skip if surrounding context suggests this isn't salary
            start = max(0, match.start() - 60)
            end = min(len(text), match.end() + 30)
            context = text[start:end]
            if _NEGATIVE_CONTEXT.search(context) and not re.search(r"salary|compensation|pay|wage|base", context, re.IGNORECASE):
                continue

            groups = match.groups()
            try:
                val_min = _parse_number(groups[0])
                val_max = _parse_number(groups[1])
            except (ValueError, IndexError):
                continue

            period = _detect_period(raw)
            # If no explicit period marker, infer from magnitude
            if period == "yearly" and not re.search(r"year|yr|annum|annual", raw, re.IGNORECASE):
                period = _infer_period_from_value(max(val_min, val_max))

            if val_min > val_max > 0:
                val_min, val_max = val_max, val_min

            if _is_reasonable_salary(val_min, val_max, period):
                return {
                    "min": int(val_min),
                    "max": int(val_max),
                    "period": period,
                    "raw": raw.strip(),
                }

    # Try single value pattern (less reliable)
    for match in _PAT_SINGLE.finditer(text):
        raw = match.group(0)

        start = max(0, match.start() - 60)
        end = min(len(text), match.end() + 30)
        context = text[start:end]

        # For single values, require salary-related context nearby
        if not re.search(r"salary|compensation|pay|wage|base|offer|range|rate|earn", context, re.IGNORECASE):
            continue
        if _NEGATIVE_CONTEXT.search(context) and not re.search(r"salary|compensation|pay|wage|base", context, re.IGNORECASE):
            continue

        groups = match.groups()
        try:
            val = _parse_number(groups[0])
        except (ValueError, IndexError):
            continue

        period = _detect_period(raw)
        if period == "yearly" and not re.search(r"year|yr|annum|annual", raw, re.IGNORECASE):
            period = _infer_period_from_value(val)

        if _is_reasonable_salary(val, val, period):
            return {
                "min": int(val),
                "max": int(val),
                "period": period,
                "raw": raw.strip(),
            }

    return None


def has_pay_keywords(text: str) -> bool:
    """Check if text contains pay-related keywords (triggers AI fallback)."""
    if not text:
        return False
    return bool(_PAY_KEYWORDS.search(text))


async def extract_salary_ai(description: str) -> Optional[dict]:
    """
    AI fallback: ask Haiku to extract salary from job description.
    Only called when regex finds nothing but pay keywords are present.
    Returns {"min": int, "max": int, "period": str, "raw": str} or None.
    """
    if not description or len(description) < 50:
        return None

    try:
        from config import settings
        import httpx

        # Truncate description to save tokens — salary info is usually near top or bottom
        truncated = description[:2000]
        if len(description) > 2000:
            truncated += "\n...\n" + description[-1000:]

        prompt = f"""Extract the salary/pay information from this job description.
Return ONLY a JSON object with these fields:
- "min": minimum salary as a number (0 if not found)
- "max": maximum salary as a number (0 if not found)
- "period": "yearly", "hourly", "monthly", or "weekly"
- "raw": the exact text snippet where you found the salary

If no salary/pay information is found, return {{"min": 0, "max": 0, "period": "yearly", "raw": ""}}

Job description:
{truncated}"""

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": settings.anthropic_api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 200,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()
            text = data["content"][0]["text"].strip()

            # Parse JSON from response (handle markdown code blocks)
            if text.startswith("```"):
                text = re.sub(r"^```\w*\n?", "", text)
                text = re.sub(r"\n?```$", "", text)
            result = json.loads(text)

            val_min = int(float(result.get("min", 0)))
            val_max = int(float(result.get("max", 0)))
            period = result.get("period", "yearly")
            raw = result.get("raw", "")

            if val_min == 0 and val_max == 0:
                return None

            if period not in ("yearly", "hourly", "monthly", "weekly"):
                period = "yearly"

            if val_min > val_max > 0:
                val_min, val_max = val_max, val_min

            if _is_reasonable_salary(val_min, val_max, period):
                return {"min": val_min, "max": val_max, "period": period, "raw": raw}

    except Exception as e:
        logger.debug(f"[Salary AI] Extraction failed: {e}")

    return None


async def extract_salary(description: str, existing_min: int = 0, existing_max: int = 0) -> Optional[dict]:
    """
    Main entry point. Try regex first, fall back to AI if pay keywords exist.
    Skip entirely if salary data already exists from the source.
    Returns {"min": int, "max": int, "period": str, "raw": str} or None.
    """
    # Already has salary from source — don't override
    if existing_min > 0 or existing_max > 0:
        return None

    if not description:
        return None

    # Stage 1: regex
    result = extract_salary_regex(description)
    if result:
        logger.debug(f"[Salary] Regex extracted: {result['raw']} → ${result['min']}-${result['max']}/{result['period']}")
        return result

    # Stage 2: AI fallback (only if pay keywords present)
    if has_pay_keywords(description):
        result = await extract_salary_ai(description)
        if result:
            logger.debug(f"[Salary AI] Extracted: {result['raw']} → ${result['min']}-${result['max']}/{result['period']}")
            return result

    return None
