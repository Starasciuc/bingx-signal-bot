import os
from typing import Literal, Optional

import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field


app = FastAPI(title="Fast Futures Signal Bot")


# =========================
# TELEGRAM CONFIG
# =========================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


# =========================
# TYPES
# =========================

Direction = Literal["LONG", "SHORT"]
SignalStatus = Literal["ACTIVE", "WAIT", "FAKEOUT_RISK", "CANCELLED"]


# =========================
# INPUT MODEL
# =========================

class FuturesSignalInput(BaseModel):
    symbol: str = Field(default="NEAR/USDT")
    direction: Direction = Field(default="LONG")

    entry_min: float = Field(default=0.0)
    entry_max: float = Field(default=0.0)
    stop_loss: float = Field(default=0.0)

    tp1_percent: float = Field(default=4.0)
    tp2_percent: float = Field(default=8.0)
    tp3_percent: float = Field(default=15.0)

    deposit: float = Field(default=1000.0)
    risk_percent: float = Field(default=0.5)

    leverage_min: int = Field(default=5)
    leverage_max: int = Field(default=10)

    timeframe_entry: str = Field(default="5m / 15m")
    timeframe_filter: str = Field(default="1H")

    # Подтверждения сигнала
    trend_confirmed: bool = Field(default=True)
    volume_confirmed: bool = Field(default=True)
    cvd_confirmed: bool = Field(default=True)
    btc_confirmed: bool = Field(default=True)
    orderbook_clear: bool = Field(default=True)
    funding_ok: bool = Field(default=True)
    oi_confirmed: bool = Field(default=True)

    # Защита от ложных сигналов
    fakeout_detected: bool = Field(default=False)
    absorption_detected: bool = Field(default=False)
    candle_closed_against_signal: bool = Field(default=False)
    btc_opposite_move: bool = Field(default=False)
    cvd_opposite: bool = Field(default=False)
    volume_weak: bool = Field(default=False)

    # Отправка в Telegram
    send_to_telegram: bool = Field(default=False)

    # True = короткий формат, False = полный формат
    compact: bool = Field(default=True)


# =========================
# SCORE LOGIC
# =========================

def calculate_score(data: FuturesSignalInput) -> int:
    score = 0

    if data.trend_confirmed:
        score += 20

    if data.volume_confirmed:
        score += 15

    if data.cvd_confirmed:
        score += 15

    if data.btc_confirmed:
        score += 15

    if data.orderbook_clear:
        score += 10

    if data.oi_confirmed:
        score += 10

    if data.funding_ok:
        score += 5

    if not data.fakeout_detected:
        score += 5

    if not data.absorption_detected:
        score += 5

    # Штрафы
    if data.btc_opposite_move:
        score -= 20

    if data.cvd_opposite:
        score -= 20

    if data.volume_weak:
        score -= 15

    if data.candle_closed_against_signal:
        score -= 30

    return max(0, min(score, 100))


def detect_status(data: FuturesSignalInput, score: int) -> SignalStatus:
    if data.candle_closed_against_signal:
        return "CANCELLED"

    if data.fakeout_detected:
        return "FAKEOUT_RISK"

    if data.absorption_detected:
        return "FAKEOUT_RISK"

    if data.btc_opposite_move:
        return "FAKEOUT_RISK"

    if data.cvd_opposite:
        return "FAKEOUT_RISK"

    if data.volume_weak:
        return "WAIT"

    if score >= 80:
        return "ACTIVE"

    if 65 <= score < 80:
        return "WAIT"

    return "CANCELLED"


# =========================
# RISK LOGIC
# =========================

def calculate_position(data: FuturesSignalInput) -> dict:
    avg_entry = 0.0

    if data.entry_min > 0 and data.entry_max > 0:
        avg_entry = (data.entry_min + data.entry_max) / 2

    risk_amount = data.deposit * data.risk_percent / 100

    if avg_entry <= 0 or data.stop_loss <= 0:
        return {
            "avg_entry": round(avg_entry, 8),
            "risk_amount": round(risk_amount, 2),
            "stop_distance_percent": None,
            "position_size_usdt": None,
            "coin_amount": None,
            "margin_5x": None,
            "margin_10x": None,
            "margin_20x": None,
            "error": "Entry или Stop Loss не указаны"
        }

    if data.direction == "LONG":
        stop_distance = avg_entry - data.stop_loss
    else:
        stop_distance = data.stop_loss - avg_entry

    if stop_distance <= 0:
        return {
            "avg_entry": round(avg_entry, 8),
            "risk_amount": round(risk_amount, 2),
            "stop_distance_percent": None,
            "position_size_usdt": None,
            "coin_amount": None,
            "margin_5x": None,
            "margin_10x": None,
            "margin_20x": None,
            "error": "Stop Loss стоит неправильно для выбранного направления"
        }

    stop_distance_percent = abs(stop_distance / avg_entry) * 100
    coin_amount = risk_amount / stop_distance
    position_size_usdt = coin_amount * avg_entry

    margin_5x = position_size_usdt / 5
    margin_10x = position_size_usdt / 10
    margin_20x = position_size_usdt / 20

    return {
        "avg_entry": round(avg_entry, 8),
        "risk_amount": round(risk_amount, 2),
        "stop_distance_percent": round(stop_distance_percent, 2),
        "position_size_usdt": round(position_size_usdt, 2),
        "coin_amount": round(coin_amount, 8),
        "margin_5x": round(margin_5x, 2),
        "margin_10x": round(margin_10x, 2),
        "margin_20x": round(margin_20x, 2),
        "error": None
    }


def calculate_tps(data: FuturesSignalInput) -> dict:
    if data.entry_min <= 0 or data.entry_max <= 0:
        return {
            "tp1": 0.0,
            "tp2": 0.0,
            "tp3": 0.0
        }

    if data.direction == "LONG":
        base = data.entry_max
        tp1 = base * (1 + data.tp1_percent / 100)
        tp2 = base * (1 + data.tp2_percent / 100)
        tp3 = base * (1 + data.tp3_percent / 100)
    else:
        base = data.entry_min
        tp1 = base * (1 - data.tp1_percent / 100)
        tp2 = base * (1 - data.tp2_percent / 100)
        tp3 = base * (1 - data.tp3_percent / 100)

    return {
        "tp1": round(tp1, 8),
        "tp2": round(tp2, 8),
        "tp3": round(tp3, 8)
    }


# =========================
# MESSAGE FORMAT
# =========================

def status_emoji(status: SignalStatus) -> str:
    if status == "ACTIVE":
        return "🟢"
    if status == "WAIT":
        return "🟡"
    if status == "FAKEOUT_RISK":
        return "⚠️"
    return "🔴"


def direction_emoji(direction: Direction) -> str:
    return "📈" if direction == "LONG" else "📉"


def bool_icon(value: bool) -> str:
    return "✅" if value else "❌"


def build_verdict(status: SignalStatus) -> str:
    if status == "ACTIVE":
        return "Вход разрешён. Сигнал подтверждён."
    if status == "WAIT":
        return "Ждём подтверждения. Вход пока запрещён."
    if status == "FAKEOUT_RISK":
        return "Вход запрещён. Есть риск ложного движения / откупа."
    return "Сигнал отменён. Условия входа сломаны."


def build_compact_message(data: FuturesSignalInput) -> str:
    score = calculate_score(data)
    status = detect_status(data, score)
    pos = calculate_position(data)
    tps = calculate_tps(data)

    emoji = status_emoji(status)
    dir_emoji = direction_emoji(data.direction)

    if pos["error"]:
        risk_line = f"⚠️ RM Error: {pos['error']}"
        size_line = ""
    else:
        risk_line = f"⚠️ Risk: {data.risk_percent}% = {pos['risk_amount']} USDT"
        size_line = (
            f"📦 Position: {pos['position_size_usdt']} USDT\n"
            f"💵 Margin: {pos['margin_5x']} USDT x5 / "
            f"{pos['margin_10x']} USDT x10 / "
            f"{pos['margin_20x']} USDT x20"
        )

    msg = f"""
{emoji} #{data.symbol} {data.direction} {dir_emoji}

📊 Status: {status}
🎯 Score: {score}/100
⏱ TF: {data.timeframe_entry}
🔎 Filter: {data.timeframe_filter}

📍 Entry: {data.entry_min} — {data.entry_max}
🛑 SL: {data.stop_loss}

🎯 TP1: {tps['tp1']}  (+{data.tp1_percent}%)
🎯 TP2: {tps['tp2']}  (+{data.tp2_percent}%)
🚀 TP3: {tps['tp3']}  (+{data.tp3_percent}%+)

⚙️ Leverage: x{data.leverage_min}–x{data.leverage_max}
{risk_line}
{size_line}

✅ Confirm:
Trend {bool_icon(data.trend_confirmed)} | Volume {bool_icon(data.volume_confirmed)} | CVD {bool_icon(data.cvd_confirmed)}
BTC {bool_icon(data.btc_confirmed)} | OI {bool_icon(data.oi_confirmed)} | OB {bool_icon(data.orderbook_clear)}

🛡 Fakeout:
Fakeout {"❌" if data.fakeout_detected else "✅"} | Absorption {"❌" if data.absorption_detected else "✅"} | BTC Opposite {"❌" if data.btc_opposite_move else "✅"}

🧠 Verdict:
{build_verdict(status)}

❌ Cancel:
Price back behind entry level / BTC against / CVD opposite / absorption detected
""".strip()

    return msg


def build_full_message(data: FuturesSignalInput) -> str:
    score = calculate_score(data)
    status = detect_status(data, score)
    pos = calculate_position(data)
    tps = calculate_tps(data)

    emoji = status_emoji(status)
    dir_emoji = direction_emoji(data.direction)

    if pos["error"]:
        risk_block = f"""
🧮 Риск-менеджмент:
• Депозит: {data.deposit} USDT
• Риск: {data.risk_percent}%
• Максимальная потеря: {pos['risk_amount']} USDT
• Ошибка: {pos['error']}
"""
    else:
        risk_block = f"""
🧮 Риск-менеджмент:
• Депозит: {data.deposit} USDT
• Риск на сделку: {data.risk_percent}%
• Максимальная потеря: {pos['risk_amount']} USDT
• Средний вход: {pos['avg_entry']}
• Дистанция до SL: {pos['stop_distance_percent']}%
• Размер позиции: {pos['position_size_usdt']} USDT
• Количество монет: {pos['coin_amount']}

💵 Маржа:
• x5: {pos['margin_5x']} USDT
• x10: {pos['margin_10x']} USDT
• x20: {pos['margin_20x']} USDT
"""

    msg = f"""
━━━━━━━━━━━━━━━━━━━━
{emoji} {data.symbol} — {data.direction} {dir_emoji}
━━━━━━━━━━━━━━━━━━━━

📊 Статус: {status}
🎯 Score: {score}/100
⏱ Входной TF: {data.timeframe_entry}
🔎 Фильтр тренда: {data.timeframe_filter}

📍 Зона входа:
{data.entry_min} — {data.entry_max}

🛑 Stop Loss:
{data.stop_loss}

🎯 Take Profit:
TP1: {tps['tp1']}  (+{data.tp1_percent}%)
TP2: {tps['tp2']}  (+{data.tp2_percent}%)
TP3: {tps['tp3']}  (+{data.tp3_percent}%+)

⚙️ Плечо:
Рекомендовано: x{data.leverage_min}–x{data.leverage_max}
x20: только если риск позиции не выше {data.risk_percent}% депозита

{risk_block}

━━━━━━━━━━━━━━━━━━━━
✅ Подтверждения:
━━━━━━━━━━━━━━━━━━━━
{bool_icon(data.trend_confirmed)} Тренд подтверждает {data.direction}
{bool_icon(data.volume_confirmed)} Объём подтверждает движение
{bool_icon(data.cvd_confirmed)} CVD / Delta подтверждает направление
{bool_icon(data.btc_confirmed)} BTC / рынок не против сигнала
{bool_icon(data.orderbook_clear)} В стакане нет сильного встречного давления
{bool_icon(data.funding_ok)} Funding не перегрет
{bool_icon(data.oi_confirmed)} Open Interest подтверждает движение

━━━━━━━━━━━━━━━━━━━━
🛡 Защита от ложного сигнала:
━━━━━━━━━━━━━━━━━━━━
{"❌" if data.fakeout_detected else "✅"} Ложный пробой не обнаружен
{"❌" if data.absorption_detected else "✅"} Откуп / absorption не обнаружен
{"❌" if data.candle_closed_against_signal else "✅"} Свеча не закрылась против сигнала
{"❌" if data.btc_opposite_move else "✅"} BTC не идёт против сигнала
{"❌" if data.cvd_opposite else "✅"} CVD не идёт против сигнала
{"❌" if data.volume_weak else "✅"} Объём не слабый

━━━━━━━━━━━━━━━━━━━━
🧠 Решение бота:
━━━━━━━━━━━━━━━━━━━━
{build_verdict(status)}

━━━━━━━━━━━━━━━━━━━━
❌ Условия отмены:
━━━━━━━━━━━━━━━━━━━━
• Цена возвращается за пробитый уровень
• CVD разворачивается против сделки
• BTC резко идёт против направления
• Появляется сильный откуп / absorption
• Объём не подтверждает движение
• Свеча закрывается против сигнала
━━━━━━━━━━━━━━━━━━━━
""".strip()

    return msg


def build_message(data: FuturesSignalInput) -> str:
    if data.compact:
        return build_compact_message(data)
    return build_full_message(data)


# =========================
# TELEGRAM
# =========================

def send_telegram_message(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {
            "ok": False,
            "error": "TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не указаны в Render Environment Variables"
        }

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    }

    try:
        response = requests.post(url, json=payload, timeout=15)

        try:
            return response.json()
        except Exception:
            return {
                "ok": False,
                "error": response.text
            }

    except Exception as e:
        return {
            "ok": False,
            "error": str(e)
        }


# =========================
# ROUTES
# =========================

@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Fast Futures Signal Bot</title>
    <style>
        body {
            background: #020617;
            color: #e5e7eb;
            font-family: Arial, sans-serif;
            padding: 40px;
        }
        .card {
            max-width: 900px;
            margin: auto;
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 20px;
            padding: 30px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.45);
        }
        h1 {
            color: #22c55e;
        }
        h2 {
            color: #38bdf8;
        }
        pre {
            background: #020617;
            border: 1px solid #1e293b;
            padding: 18px;
            border-radius: 12px;
            overflow-x: auto;
            color: #cbd5e1;
        }
        .ok {
            color: #22c55e;
            font-weight: bold;
        }
        .warn {
            color: #facc15;
            font-weight: bold;
        }
    </style>
</head>
<body>
    <div class="card">
        <h1>Fast Futures Signal Bot</h1>
        <p class="ok">✅ Бот работает на Render</p>
        <p>Это API-фильтр сигнала. Он принимает данные, считает score, риск, TP/SL и может отправлять результат в Telegram.</p>

        <h2>Endpoints</h2>
        <pre>GET /health
GET /preview
GET /preview-text
POST /signal</pre>

        <h2>Пример POST /signal</h2>
        <pre>{
  "symbol": "NEAR/USDT",
  "direction": "LONG",
  "entry_min": 7.10,
  "entry_max": 7.15,
  "stop_loss": 7.03,
  "tp1_percent": 4,
  "tp2_percent": 8,
  "tp3_percent": 15,
  "deposit": 1000,
  "risk_percent": 0.5,
  "leverage_min": 5,
  "leverage_max": 10,
  "timeframe_entry": "5m / 15m",
  "timeframe_filter": "1H",
  "trend_confirmed": true,
  "volume_confirmed": true,
  "cvd_confirmed": true,
  "btc_confirmed": true,
  "orderbook_clear": true,
  "funding_ok": true,
  "oi_confirmed": true,
  "fakeout_detected": false,
  "absorption_detected": false,
  "candle_closed_against_signal": false,
  "btc_opposite_move": false,
  "cvd_opposite": false,
  "volume_weak": false,
  "send_to_telegram": true,
  "compact": true
}</pre>

        <p class="warn">Важно: если fakeout, absorption, BTC против или CVD против — сигнал не станет ACTIVE.</p>
    </div>
</body>
</html>
"""


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "Fast Futures Signal Bot",
        "file": "bot.py"
    }


@app.get("/preview")
def preview():
    data = FuturesSignalInput(
        symbol="NEAR/USDT",
        direction="LONG",
        entry_min=7.10,
        entry_max=7.15,
        stop_loss=7.03,
        tp1_percent=4,
        tp2_percent=8,
        tp3_percent=15,
        deposit=1000,
        risk_percent=0.5,
        leverage_min=5,
        leverage_max=10,
        timeframe_entry="5m / 15m",
        timeframe_filter="1H",
        trend_confirmed=True,
        volume_confirmed=True,
        cvd_confirmed=True,
        btc_confirmed=True,
        orderbook_clear=True,
        funding_ok=True,
        oi_confirmed=True,
        fakeout_detected=False,
        absorption_detected=False,
        candle_closed_against_signal=False,
        btc_opposite_move=False,
        cvd_opposite=False,
        volume_weak=False,
        send_to_telegram=False,
        compact=True
    )

    score = calculate_score(data)
    status = detect_status(data, score)

    return {
        "symbol": data.symbol,
        "direction": data.direction,
        "status": status,
        "score": score,
        "risk": calculate_position(data),
        "take_profits": calculate_tps(data),
        "message": build_message(data)
    }


@app.get("/preview-text", response_class=PlainTextResponse)
def preview_text():
    data = FuturesSignalInput(
        symbol="NEAR/USDT",
        direction="LONG",
        entry_min=7.10,
        entry_max=7.15,
        stop_loss=7.03,
        tp1_percent=4,
        tp2_percent=8,
        tp3_percent=15,
        deposit=1000,
        risk_percent=0.5,
        leverage_min=5,
        leverage_max=10,
        timeframe_entry="5m / 15m",
        timeframe_filter="1H",
        trend_confirmed=True,
        volume_confirmed=True,
        cvd_confirmed=True,
        btc_confirmed=True,
        orderbook_clear=True,
        funding_ok=True,
        oi_confirmed=True,
        fakeout_detected=False,
        absorption_detected=False,
        candle_closed_against_signal=False,
        btc_opposite_move=False,
        cvd_opposite=False,
        volume_weak=False,
        send_to_telegram=False,
        compact=True
    )

    return build_message(data)


@app.post("/signal")
def create_signal(data: FuturesSignalInput):
    score = calculate_score(data)
    status = detect_status(data, score)
    risk = calculate_position(data)
    tps = calculate_tps(data)
    message = build_message(data)

    telegram_result: Optional[dict] = None

    if data.send_to_telegram:
        telegram_result = send_telegram_message(message)

    return {
        "symbol": data.symbol,
        "direction": data.direction,
        "status": status,
        "score": score,
        "entry": {
            "min": data.entry_min,
            "max": data.entry_max
        },
        "stop_loss": data.stop_loss,
        "take_profits": tps,
        "risk": risk,
        "message": message,
        "telegram": telegram_result
    }
