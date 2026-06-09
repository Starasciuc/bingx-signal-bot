from pathlib import Path
import re
import sys

SRC = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('bot.py')
OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else Path('bot_v6_0_professional_balanced.py')

text = SRC.read_text(encoding='utf-8')

# Version / marker
text = re.sub(
    r'APP_NAME = "[^"]+"',
    'APP_NAME = "Professional Adaptive Futures Bot AUTO V6.0 PROFESSIONAL BALANCED HTF + Anti-Chase"',
    text,
    count=1,
)
text = re.sub(
    r'DEPLOY_MARKER = "[^"]+"',
    'DEPLOY_MARKER = "V6.0_PROFESSIONAL_BALANCED_2026_06_09"',
    text,
    count=1,
)

# Professional but not dead thresholds.
repls = {
    'B_MIN_SCORE = strategy_int("B_MIN_SCORE", 73)': 'B_MIN_SCORE = strategy_int("B_MIN_SCORE", 76)',
    'B_MIN_VOLUME_RATIO = strategy_float("B_MIN_VOLUME_RATIO", 0.95)': 'B_MIN_VOLUME_RATIO = strategy_float("B_MIN_VOLUME_RATIO", 1.02)',
    'B_MIN_RR = strategy_float("B_MIN_RR", 0.58)': 'B_MIN_RR = strategy_float("B_MIN_RR", 0.70)',
    'B_RISK_MULTIPLIER = strategy_float("B_RISK_MULTIPLIER", 0.22)': 'B_RISK_MULTIPLIER = strategy_float("B_RISK_MULTIPLIER", 0.20)',
    'LEVEL_B_MIN_SCORE = strategy_int("LEVEL_B_MIN_SCORE", 71)': 'LEVEL_B_MIN_SCORE = strategy_int("LEVEL_B_MIN_SCORE", 74)',
    'LEVEL_B_MIN_VOLUME_RATIO = strategy_float("LEVEL_B_MIN_VOLUME_RATIO", 0.92)': 'LEVEL_B_MIN_VOLUME_RATIO = strategy_float("LEVEL_B_MIN_VOLUME_RATIO", 1.00)',
    'LEVEL_B_MIN_RR = strategy_float("LEVEL_B_MIN_RR", 0.52)': 'LEVEL_B_MIN_RR = strategy_float("LEVEL_B_MIN_RR", 0.65)',
    'IMPULSE_PULLBACK_RISK_MULTIPLIER = float(os.getenv("IMPULSE_PULLBACK_RISK_MULTIPLIER", "0.22"))': 'IMPULSE_PULLBACK_RISK_MULTIPLIER = float(os.getenv("IMPULSE_PULLBACK_RISK_MULTIPLIER", "0.20"))',
    'IMPULSE_MIN_MOVE_5M_PERCENT = float(os.getenv("IMPULSE_MIN_MOVE_5M_PERCENT", "0.75"))': 'IMPULSE_MIN_MOVE_5M_PERCENT = float(os.getenv("IMPULSE_MIN_MOVE_5M_PERCENT", "0.90"))',
    'IMPULSE_MIN_VOLUME_RATIO = float(os.getenv("IMPULSE_MIN_VOLUME_RATIO", "0.85"))': 'IMPULSE_MIN_VOLUME_RATIO = float(os.getenv("IMPULSE_MIN_VOLUME_RATIO", "1.00"))',
}
for old, new in repls.items():
    if old in text:
        text = text.replace(old, new)
    else:
        print(f"WARN: not found: {old}")

# Insert professional guard constants after HTF_NO_CONFIRM_B_RISK_MULTIPLIER.
needle = 'HTF_NO_CONFIRM_B_RISK_MULTIPLIER = float(os.getenv("HTF_NO_CONFIRM_B_RISK_MULTIPLIER", "0.14"))\n'
insert = needle + '''
# V6.0 Professional Balanced Guard.
# Цель: не убить сигналы полностью, но убрать слабые SHORT/B+ и входы против старшего движения.
PRO_BALANCED_GUARD_ENABLED = os.getenv("PRO_BALANCED_GUARD_ENABLED", "true").lower() == "true"
SHORT_B_ENABLED = os.getenv("SHORT_B_ENABLED", "true").lower() == "true"
SHORT_B_MIN_SCORE = strategy_int("SHORT_B_MIN_SCORE", 82)
SHORT_B_MIN_RR = strategy_float("SHORT_B_MIN_RR", 0.78)
SHORT_B_MIN_VOLUME_RATIO = strategy_float("SHORT_B_MIN_VOLUME_RATIO", 1.08)
SHORT_A_PLUS_BLOCK_IF_BTC_BULLISH = os.getenv("SHORT_A_PLUS_BLOCK_IF_BTC_BULLISH", "true").lower() == "true"
SHORT_BLOCK_IF_1H_BULLISH = os.getenv("SHORT_BLOCK_IF_1H_BULLISH", "true").lower() == "true"
SHORT_B_REQUIRES_1H_OR_4H_BEARISH = os.getenv("SHORT_B_REQUIRES_1H_OR_4H_BEARISH", "true").lower() == "true"
LONG_BLOCK_IF_BTC_BEARISH_AND_1H_BEARISH = os.getenv("LONG_BLOCK_IF_BTC_BEARISH_AND_1H_BEARISH", "true").lower() == "true"
'''
if needle in text and 'PRO_BALANCED_GUARD_ENABLED' not in text:
    text = text.replace(needle, insert)
else:
    print("WARN: HTF constants insertion skipped")

# Add helper function after htf_confirmation_data.
needle2 = '''def attach_htf_confirmation(filters: dict, direction: str, market_data: dict) -> dict:\n'''
helper = '''

def pro_direction_guard_allows(score: int, rr: float, volume: float, filters: dict, direction: str, wanted_grade: str) -> bool:
    """
    V6.0 guard: score не может перебить плохой старший контекст.
    Но LONG/B не душим слишком сильно, чтобы бот не молчал.
    """
    if not PRO_BALANCED_GUARD_ENABLED:
        return True

    btc = filters.get("btc_status", "NEUTRAL")
    trend1h = filters.get("trend1h", "NEUTRAL")
    trend4h = filters.get("trend4h", "NEUTRAL")

    if direction == "SHORT":
        # Не шортим против явного BTC/1H импульса вверх.
        if btc == "BULLISH" and SHORT_A_PLUS_BLOCK_IF_BTC_BULLISH:
            return False
        if trend1h == "BULLISH" and SHORT_BLOCK_IF_1H_BULLISH:
            return False

        if wanted_grade == "B":
            if not SHORT_B_ENABLED:
                return False
            if score < SHORT_B_MIN_SCORE or rr < SHORT_B_MIN_RR or volume < SHORT_B_MIN_VOLUME_RATIO:
                return False
            if SHORT_B_REQUIRES_1H_OR_4H_BEARISH and not (trend1h in ["BEARISH", "SOFT_BEARISH"] or trend4h in ["BEARISH", "SOFT_BEARISH"]):
                return False

    if direction == "LONG":
        # LONG оставляем живым, но не берём, если BTC и 1H одновременно bearish.
        if LONG_BLOCK_IF_BTC_BEARISH_AND_1H_BEARISH and btc == "BEARISH" and trend1h == "BEARISH":
            return False

    return True
'''
if needle2 in text and 'def pro_direction_guard_allows' not in text:
    text = text.replace(needle2, helper + "\n" + needle2)
else:
    print("WARN: helper insertion skipped")

# Patch classify_signal: add guard before A+ return and B return.
old_aplus = '''    if (
        score >= A_PLUS_MIN_SCORE
        and rr >= A_PLUS_MIN_RR
        and volume >= A_PLUS_MIN_VOLUME_RATIO
        and not funding.get("blocked")
        and can_strategy_be_a_plus(strategy, direction)
        and level_a_plus_allowed
        and a_plus_htf_allowed
    ):
        return {"grade": "A+", "risk_multiplier": A_PLUS_RISK_MULTIPLIER}
'''
new_aplus = '''    if (
        score >= A_PLUS_MIN_SCORE
        and rr >= A_PLUS_MIN_RR
        and volume >= A_PLUS_MIN_VOLUME_RATIO
        and not funding.get("blocked")
        and can_strategy_be_a_plus(strategy, direction)
        and level_a_plus_allowed
        and a_plus_htf_allowed
        and pro_direction_guard_allows(score, rr, volume, filters, direction, "A+")
    ):
        return {"grade": "A+", "risk_multiplier": A_PLUS_RISK_MULTIPLIER}
'''
if old_aplus in text:
    text = text.replace(old_aplus, new_aplus)
else:
    print("WARN: A+ classify block not found")

old_b = '''    if score >= b_score and rr >= b_rr and volume >= b_vol and not funding.get("blocked"):
        if HTF_CONFIRMATION_ENABLED and B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM and not htf_any:
            return None
        return {"grade": "B", "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER)}
'''
new_b = '''    if score >= b_score and rr >= b_rr and volume >= b_vol and not funding.get("blocked"):
        if HTF_CONFIRMATION_ENABLED and B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM and not htf_any:
            return None
        if not pro_direction_guard_allows(score, rr, volume, filters, direction, "B"):
            return None
        return {"grade": "B", "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER)}
'''
if old_b in text:
    text = text.replace(old_b, new_b)
else:
    print("WARN: B classify block not found")

# Also patch force_grade B path with guard.
old_force = '''    if filters.get("force_grade") == "B":
        if score >= b_score and rr >= b_rr and volume >= b_vol and not funding.get("blocked"):
            if HTF_CONFIRMATION_ENABLED and B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM and not htf_any:
                # Чтобы бот не молчал полностью, можно оставить такой сетап только если он очень сильный,
                # но с ещё меньшим риском. По умолчанию лучше не пропускать без HTF.
                return None
            return {"grade": "B", "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER)}
        return None
'''
new_force = '''    if filters.get("force_grade") == "B":
        if score >= b_score and rr >= b_rr and volume >= b_vol and not funding.get("blocked"):
            if HTF_CONFIRMATION_ENABLED and B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM and not htf_any:
                # Чтобы бот не молчал полностью, можно оставить такой сетап только если он очень сильный,
                # но с ещё меньшим риском. По умолчанию лучше не пропускать без HTF.
                return None
            if not pro_direction_guard_allows(score, rr, volume, filters, direction, "B"):
                return None
            return {"grade": "B", "risk_multiplier": filters.get("risk_multiplier_override", B_RISK_MULTIPLIER)}
        return None
'''
if old_force in text:
    text = text.replace(old_force, new_force)
else:
    print("WARN: force B block not found")

# Tighten resistance reject short with second confirmation.
old_reject = '''    if not (swept and rejected and rejection):
        return None
    if d["rs5"] < 24 or d["rs15"] < 28:
        return None
'''
new_reject = '''    if not (swept and rejected and rejection):
        return None

    # V6.0: SHORT от сопротивления не берём по первой красной свече.
    # Нужен второй признак, что продавец реально забрал контроль после sweep.
    closes_below_level = sum(1 for c in c5[-4:] if c["close"] < level * 0.9995)
    price_away_from_sweep_high = price < sweep_high * 0.996
    lower_high_after_sweep = max(c["high"] for c in c5[-3:]) < sweep_high * 0.999
    if closes_below_level < 2 or not (price_away_from_sweep_high or lower_high_after_sweep):
        return None

    if d["rs5"] < 24 or d["rs15"] < 28:
        return None
'''
if old_reject in text:
    text = text.replace(old_reject, new_reject)
else:
    print("WARN: resistance reject confirm block not found")

# Fresh breakout/breakdown should be more cautious.
text = text.replace(
    'filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.20)\n        filters["anti_fakeout_note"] = "Fresh breakout без идеального ретеста: разрешён только B с малым риском."',
    'filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.16)\n        filters["anti_fakeout_note"] = "Fresh breakout без идеального ретеста: только B с малым риском. A+ запрещён."'
)
text = text.replace(
    'filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.20)\n        filters["anti_fakeout_note"] = "Fresh breakdown без идеального ретеста: разрешён только B с малым риском."',
    'filters["risk_multiplier_override"] = min(filters.get("risk_multiplier_override", B_RISK_MULTIPLIER), 0.16)\n        filters["anti_fakeout_note"] = "Fresh breakdown без идеального ретеста: только B с малым риском. A+ запрещён."'
)

# Startup text update.
text = text.replace(
    '"V5.9 цель: A+ уверенный, B+ среднеуверенный, без догоняющих входов после 5-10% движения."',
    '"V6.0 цель: профессиональный баланс — сигналы остаются, но слабые SHORT/B+ и догоняющие входы режутся жёстче."'
)
text = text.replace(
    'f"HTF 1H/4H confirm: {\'ON\' if HTF_CONFIRMATION_ENABLED else \'OFF\'} | A+ both TF: {\'ON\' if A_PLUS_REQUIRES_1H_4H_CONFIRM else \'OFF\'} | B any TF: {\'ON\' if B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM else \'OFF\'}\\n"',
    'f"HTF 1H/4H confirm: {\'ON\' if HTF_CONFIRMATION_ENABLED else \'OFF\'} | A+ both TF: {\'ON\' if A_PLUS_REQUIRES_1H_4H_CONFIRM else \'OFF\'} | B any TF: {\'ON\' if B_REQUIRES_AT_LEAST_ONE_HTF_CONFIRM else \'OFF\'}\\n"\n        f"V6 Pro Guard: {\'ON\' if PRO_BALANCED_GUARD_ENABLED else \'OFF\'} | SHORT B: {\'ON\' if SHORT_B_ENABLED else \'OFF\'} / {SHORT_B_MIN_SCORE}+ / RR {SHORT_B_MIN_RR} / vol x{SHORT_B_MIN_VOLUME_RATIO}\\n"'
)

OUT.write_text(text, encoding='utf-8')
print(f"Saved patched bot to: {OUT}")
