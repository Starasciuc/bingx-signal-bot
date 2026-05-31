import os
import requests
import uvicorn
from typing import Literal
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field


app = FastAPI(title="BingX Fast Futures Signal Bot")


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


Direction = Literal["LONG", "SHORT"]
SignalStatus = Literal["ACTIVE", "WAIT", "FAKEOUT_RISK", "CANCELLED"]


class SignalInput(BaseModel):
    symbol: str = Field(default="NEAR/USDT")
    direction: Direction = Field(default="LONG")

    entry_min: float = Field(default=7.10)
    entry_max: float = Field(default=7.15)
    stop_loss: float = Field(default=7.03)

    tp1_percent: float = Field(default=4.0)
    tp2_percent: float = Field(default=8.0)
    tp3_percent: float = Field(default=15.0)

    deposit: float = Field(default=1000.0)
    risk_percent: float = Field(default=0.5)

    leverage_min: int = Field(default=5)
    leverage_max: int = Field(default=10)

    timeframe_entry: str = Field(default="5m / 15m")
    timeframe_filter: str = Field(default="1H")

    trend_confirmed: bool = Field(default=True)
    volume_confirmed: bool = Field(default=True)
    cvd_confirmed: bool = Field(default=True)
    btc_confirmed: bool = Field(default=True)
    orderbook_clear: bool = Field(default=True)
    funding_ok: bool = Field(default=True)
    oi_confirmed: bool = Field(default=True)

    fakeout_detected: bool = Field(default=False)
    absorption_detected: bool = Field(default=False)
    candle_closed_against_signal: bool = Field(default=False)
    btc_opposite_move: bool = Field(default=False)
    cvd_opposite: bool = Field(default=False)
    volume_weak: bool = Field(default=False)

    send_to_telegram: bool = Field(default=False)


def calculate_score(data: SignalInput) -> int:
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

    if data.btc_opposite_move:
        score -= 20
    if data.cvd_opposite:
        score -= 20
    if data.volume_weak:
        score -= 15
    if data.candle_closed_against_signal:
        score -= 30

    return max(0, min(score, 100))


def detect_status(data: SignalInput, score: int) -> SignalStatus:
    if data.fakeout_detected:
        return "FAKEOUT_RISK"
    if data.absorption_detected:
        return "FAKEOUT_RISK"
    if data.btc_opposite_move:
        return "FAKEOUT_RISK"
    if data.cvd_opposite:
        return "FAKEOUT_RISK"
    if data.candle_closed_against_signal:
        return "CANCELLED"
    if data.volume_weak:
        return "WAIT"

    if score >= 80:
        return "ACTIVE"
    if score >= 65:
        return "WAIT"

    return "CANCELLED"


def calculate_position(data: SignalInput) -> dict:
    avg_entry = (data.entry_min + data.entry_max) / 2
    risk_amount = data.deposit * data.risk_percent / 100

    if data.direction == "LONG":
        stop_distance = avg_entry - data.stop_loss
    else:
        stop_distance = data.stop_loss - avg_entry

    if stop_distance <= 0:
        return {
            "error": "Stop Loss стоит неправильно для выбранного направления",
            "risk_amount": round(risk_amount, 2),
        }

    coin_amount = risk_amount / stop_distance
    position_size_usdt = coin_amount * avg_entry
    stop_distance_percent = abs(stop_distance / avg_entry) * 100

    return {
        "error": None,
        "avg_entry": round(avg_entry, 6),
        "risk_amount": round(risk_amount, 2),
        "stop_distance_percent": round(stop_distance_percent, 2),
        "position_size_usdt": round(position_size_usdt, 2),
        "coin_amount": round(coin_amount, 6),
        "margin_5x": round(position_size_usdt / 5, 2),
        "margin_10x": round(position_size_usdt / 10, 2),
        "margin_20x": round(position_size_usdt / 20, 2),
    }


def calculate_tps(data: SignalInput) -> dict:
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
        "tp1": round(tp1, 6),
        "tp2": round(tp2, 6),
        "tp3": round(tp3, 6),
    }


def status_emoji(status: str) -> str:
    if status == "ACTIVE":
        return "🟢"
    if status == "WAIT":
        return "🟡"
    if status == "FAKEOUT_RISK":
        return "⚠️"
    return "🔴"


def direction_emoji(direction: str) -> str:
    return "📈" if direction == "LONG" else "📉"


def icon(value: bool) -> str:
    return "✅" if value else "❌"


def verdict(status: str) -> str:
    if status == "ACTIVE":
        return "Вход разрешён. Сигнал подтверждён."
    if status == "WAIT":
        return "Ждём подтверждения. Вход пока запрещён."
    if status == "FAKEOUT_RISK":
        return "Вход запрещён. Есть риск ложного движения / откупа."
    return "Сигнал отменён. Условия входа сломаны."


def build_message(data: SignalInput) -> str:
    score = calculate_score(data)
    status = detect_status(data, score)
    pos = calculate_position(data)
    tps = calculate_tps(data)

    if pos.get("error"):
        risk_text = f"⚠️ RM Error: {pos['error']}"
        size_text = ""
    else:
        risk_text = f"⚠️ Risk: {data.risk_percent}% = {pos['risk_amount']} USDT"
        size_text = (
            f"📦 Position: {pos['position_size_usdt']} USDT\n"
            f"💵 Margin: {pos['margin_5x']} USDT x5 / "
            f"{pos['margin_10x']} USDT x10 / "
            f"{pos['margin_20x']} USDT x20"
        )

    message = f"""
{status_emoji(status)} #{data.symbol} {data.direction} {direction_emoji(data.direction)}

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
{risk_text}
{size_text}

✅ Confirm:
Trend {icon(data.trend_confirmed)} | Volume {icon(data.volume_confirmed)} | CVD {icon(data.cvd_confirmed)}
BTC {icon(data.btc_confirmed)} | OI {icon(data.oi_confirmed)} | OB {icon(data.orderbook_clear)}

🛡 Fakeout:
Fakeout {"❌" if data.fakeout_detected else "✅"} | Absorption {"❌" if data.absorption_detected else "✅"} | BTC Opposite {"❌" if data.btc_opposite_move else "✅"}

🧠 Verdict:
{verdict(status)}

❌ Cancel:
Price back behind entry level / BTC against / CVD opposite / absorption detected
""".strip()

    return message


def send_telegram_message(text: str) -> dict:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {
            "ok": False,
            "error": "TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не указаны"
        }

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=15)
        return response.json()
    except Exception as e:
        return {
            "ok": False,
            "error": str(e)
        }


@app.get("/", response_class=HTMLResponse)
def home():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>BingX Signal Bot</title>
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
        }
        h1 { color: #22c55e; }
        h2 { color: #38bdf8; }
        pre {
            background: #020617;
            border: 1px solid #1e293b;
            padding: 18px;
            border-radius: 12px;
            overflow-x: auto;
            color: #cbd5e1;
        }
        .ok { color: #22c55e; font-weight: bold; }
        .warn { color: #facc15; font-weight: bold; }
    </style>
</head>
<body>
    <div class="card">
        <h1>BingX Fast Futures Signal Bot</h1>
        <p class="ok">✅ Бот запущен</p>

        <h2>Проверка</h2>
        <pre>/health
/preview
/preview-text</pre>

        <h2>Основной endpoint</h2>
        <pre>POST /signal</pre>

        <h2>Пример JSON</h2>
        <pre>{
  "symbol": "NEAR/USDT",
  "direction": "LONG",
  "entry_min": 7.10,
  "entry_max": 7.15,
  "stop_loss": 7.03,
  "send_to_telegram": true
}</pre>

        <p class="warn">Если fakeout, absorption, BTC opposite или CVD opposite = true, сигнал не станет ACTIVE.</p>
    </div>
</body>
</html>
"""


@app.get("/health")
def health():
    return {
        "status": "ok",
        "message": "Bot is running"
    }


@app.get("/preview")
def preview():
    data = SignalInput()
    score = calculate_score(data)
    status = detect_status(data, score)

    return {
        "status": status,
        "score": score,
        "message": build_message(data),
        "risk": calculate_position(data),
        "take_profits": calculate_tps(data)
    }


@app.get("/preview-text", response_class=PlainTextResponse)
def preview_text():
    data = SignalInput()
    return build_message(data)


@app.post("/signal")
def create_signal(data: SignalInput):
    score = calculate_score(data)
    status = detect_status(data, score)
    message = build_message(data)

    telegram_result = None
    if data.send_to_telegram:
        telegram_result = send_telegram_message(message)

    return {
        "symbol": data.symbol,
        "direction": data.direction,
        "status": status,
        "score": score,
        "message": message,
        "risk": calculate_position(data),
        "take_profits": calculate_tps(data),
        "telegram": telegram_result
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("bot:app", host="0.0.0.0", port=port)
