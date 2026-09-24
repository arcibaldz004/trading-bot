"""
mans-bots: automātisks EUR/USD bots cTrader (Pepperstone demo).
Palaiž GitHub Actions ik pēc 10 min.
- Ja nav atvērta darījuma -> atver jaunu trenda virzienā ar TP un SL.
- TP/SL aizver pats brokeris.
- Rēķina tā, it kā kontā būtu tikai VIRTUAL_BALANCE (50€).
- Kill-switch: ja virtuālais konts <= KILL_EUR, bots apstājas.
"""
import os
import json
import time
import uuid
import asyncio
import requests
import websockets

HOST = "wss://demo.ctraderapi.com:5036"   # tikai DEMO

CLIENT_ID = os.environ["CTRADER_CLIENT_ID"].strip()
CLIENT_SECRET = os.environ["CTRADER_CLIENT_SECRET"].strip()
ACCESS_TOKEN = os.environ["CTRADER_ACCESS_TOKEN"].strip()

ACCOUNT_LOGIN = int(os.environ.get("ACCOUNT_LOGIN") or 4274204)
SYMBOL_NAME = (os.environ.get("SYMBOL") or "EURUSD").upper()
VIRTUAL_BALANCE = float(os.environ.get("VIRTUAL_BALANCE") or 50)
LEVERAGE = 30
SIZE_PCT = 0.8
TP_EUR = float(os.environ.get("TP_EUR") or 2)
SL_EUR = float(os.environ.get("SL_EUR") or 3)
KILL_EUR = float(os.environ.get("KILL_EUR") or 25)
MODE = (os.environ.get("MODE") or "trade").lower()
LABEL = "mans-bots"
STATE_FILE = "bot_state.json"

TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# cTrader ziņu tipi
HEARTBEAT = 51
ERRORS = (50, 2132, 2142)
APP_AUTH_REQ, APP_AUTH_RES = 2100, 2101
ACC_AUTH_REQ, ACC_AUTH_RES = 2102, 2103
NEW_ORDER_REQ = 2106
CLOSE_POS_REQ = 2111
SYMBOLS_LIST_REQ, SYMBOLS_LIST_RES = 2114, 2115
SYMBOL_BY_ID_REQ, SYMBOL_BY_ID_RES = 2116, 2117
TRADER_REQ, TRADER_RES = 2121, 2122
RECONCILE_REQ, RECONCILE_RES = 2124, 2125
EXECUTION_EVENT = 2126
TRENDBARS_REQ, TRENDBARS_RES = 2137, 2138
ACCOUNTS_REQ, ACCOUNTS_RES = 2149, 2150
M5 = 5
BUY, SELL = 1, 2
MARKET = 1


def notify(text):
    print(text)
    if TG_TOKEN and TG_CHAT:
        try:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          json={"chat_id": TG_CHAT, "text": text}, timeout=20)
        except Exception as e:
            print("Telegram kļūda:", e)


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def ema(values, span):
    k = 2 / (span + 1)
    out, cur = [], values[0]
    for v in values:
        cur = v * k + cur * (1 - k)
        out.append(cur)
    return out


class Client:
    def __init__(self, ws):
        self.ws = ws

    async def request(self, ptype, payload, expect, timeout=25):
        expect = expect if isinstance(expect, tuple) else (expect,)
        mid = str(uuid.uuid4())
        await self.ws.send(json.dumps(
            {"clientMsgId": mid, "payloadType": ptype, "payload": payload}))
        end = time.time() + timeout
        while True:
            left = end - time.time()
            if left <= 0:
                raise TimeoutError(f"Nav atbildes uz ziņu {ptype}")
            msg = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=left))
            pt = msg.get("payloadType")
            if pt == HEARTBEAT:
                continue
            if pt in ERRORS:
                raise RuntimeError(f"cTrader kļūda: {msg.get('payload')}")
            if pt in expect:
                return msg.get("payload", {})


async def run():
    state = load_state()
    if state.get("stopped") and MODE != "test":
        print("Bots ir apturēts (kill-switch). Izdzēs bot_state.json, lai sāktu no jauna.")
        return

    async with websockets.connect(HOST, ping_interval=20, max_size=None) as ws:
        c = Client(ws)
        await c.request(APP_AUTH_REQ, {"clientId": CLIENT_ID,
                                       "clientSecret": CLIENT_SECRET}, APP_AUTH_RES)

        accs = await c.request(ACCOUNTS_REQ, {"accessToken": ACCESS_TOKEN}, ACCOUNTS_RES)
        acc = next((a for a in accs.get("ctidTraderAccount", [])
                    if int(a.get("traderLogin", 0)) == ACCOUNT_LOGIN), None)
        if not acc:
            raise RuntimeError(f"Konts {ACCOUNT_LOGIN} nav atrasts šim tokenam.")
        if acc.get("isLive"):
            raise RuntimeError("Šis ir LIVE konts. Bots pagaidām strādā tikai ar demo.")
        aid = int(acc["ctidTraderAccountId"])

        await c.request(ACC_AUTH_REQ, {"ctidTraderAccountId": aid,
                                       "accessToken": ACCESS_TOKEN}, ACC_AUTH_RES)

        trader = (await c.request(TRADER_REQ, {"ctidTraderAccountId": aid},
                                  TRADER_RES))["trader"]
        balance = trader["balance"] / 10 ** int(trader.get("moneyDigits", 2))

        syms = await c.request(SYMBOLS_LIST_REQ, {"ctidTraderAccountId": aid},
                               SYMBOLS_LIST_RES)
        light = next((s for s in syms.get("symbol", [])
                      if s.get("symbolName", "").upper() == SYMBOL_NAME), None)
        if not light:
            raise RuntimeError(f"Simbols {SYMBOL_NAME} nav atrasts.")
        sid = int(light["symbolId"])
        full = (await c.request(SYMBOL_BY_ID_REQ, {"ctidTraderAccountId": aid,
                                                   "symbolId": [sid]},
                                SYMBOL_BY_ID_RES))["symbol"][0]
        digits = int(full.get("digits", 5))
        min_vol = int(full.get("minVolume", 100000))
        step_vol = int(full.get("stepVolume", 100000))

        rec = await c.request(RECONCILE_REQ, {"ctidTraderAccountId": aid}, RECONCILE_RES)
        positions = [p for p in rec.get("position", [])
                     if int(p["tradeData"]["symbolId"]) == sid]

        now_ms = int(time.time() * 1000)
        tb = await c.request(TRENDBARS_REQ, {
            "ctidTraderAccountId": aid, "symbolId": sid, "period": M5,
            "fromTimestamp": now_ms - 4 * 24 * 3600 * 1000, "toTimestamp": now_ms,
        }, TRENDBARS_RES)
        bars = sorted(tb.get("trendbar", []),
                      key=lambda b: b["utcTimestampInMinutes"])[:-1]
        if len(bars) < 210:
            raise RuntimeError(f"Par maz datu ({len(bars)} sveces).")
        closes = [(b["low"] + b.get("deltaClose", 0)) / 100000 for b in bars]
        age_min = time.time() / 60 - bars[-1]["utcTimestampInMinutes"]

        # --- Virtuālais konts ---
        if "start_balance" not in state:
            state["start_balance"] = balance
            state["last_balance"] = balance
        virtual = VIRTUAL_BALANCE + (balance - state["start_balance"])
        delta = balance - state.get("last_balance", balance)
        if abs(delta) >= 0.01:
            emoji = "✅" if delta > 0 else "❌"
            notify(f"{emoji} Darījums aizvērts: {delta:+.2f}€\n"
                   f"Virtuālais konts: {virtual:.2f}€")
        state["last_balance"] = balance

        price = closes[-1]
        f, s, t = ema(closes, 9)[-1], ema(closes, 21)[-1], ema(closes, 200)[-1]
        if f > s and price > t:
            side = BUY
        elif f < s and price < t:
            side = SELL
        else:
            side = None

        # Pozīcijas lielums (EURUSD: bāzes valūta EUR)
        units_target = max(virtual, 0) * LEVERAGE * SIZE_PCT
        volume = int(units_target * 100) // step_vol * step_vol
        volume = max(volume, min_vol)
        units = volume / 100
        factor = 10 ** (5 - digits)
        tp_rel = max(factor, round(TP_EUR * price / units * 100000 / factor) * factor)
        sl_rel = max(factor, round(SL_EUR * price / units * 100000 / factor) * factor)

        side_txt = {BUY: "BUY 📈", SELL: "SELL 📉", None: "nav signāla"}[side]

        if MODE == "test":
            notify(f"🧪 TESTS OK\nKonts: {ACCOUNT_LOGIN} (demo)\n"
                   f"Reālā bilance: {balance:.2f}€ | virtuālā: {virtual:.2f}€\n"
                   f"{SYMBOL_NAME}: {price:.5f} (dati pirms {age_min:.0f} min)\n"
                   f"Atvērtas pozīcijas: {len(positions)}\n"
                   f"Signāls tagad: {side_txt}\n"
                   f"Lielums: {units:.0f} vienības, TP {tp_rel/10:.1f} pips, "
                   f"SL {sl_rel/10:.1f} pips")
            save_state(state)
            return

        # --- Kill-switch ---
        if virtual <= KILL_EUR:
            for p in positions:
                await c.request(CLOSE_POS_REQ, {
                    "ctidTraderAccountId": aid, "positionId": int(p["positionId"]),
                    "volume": int(p["tradeData"]["volume"])}, EXECUTION_EVENT)
            state["stopped"] = True
            save_state(state)
            notify(f"🛑 KILL-SWITCH: virtuālais konts {virtual:.2f}€. Bots apturēts.")
            return

        if age_min > 20:
            print("Tirgus slēgts vai dati novecojuši.")
            save_state(state)
            return

        if positions:
            print(f"Jau ir atvērta pozīcija ({len(positions)}), gaidu TP/SL.")
            save_state(state)
            return

        if side is None:
            print("Nav skaidra trenda, gaidu.")
            save_state(state)
            return

        await c.request(NEW_ORDER_REQ, {
            "ctidTraderAccountId": aid, "symbolId": sid, "orderType": MARKET,
            "tradeSide": side, "volume": volume,
            "relativeStopLoss": sl_rel, "relativeTakeProfit": tp_rel,
            "label": LABEL,
        }, EXECUTION_EVENT)
        notify(f"🤖 Atvērts {side_txt} {SYMBOL_NAME}\nCena ~{price:.5f}\n"
               f"Lielums: {units:.0f} | TP +{TP_EUR:.0f}€ | SL -{SL_EUR:.0f}€\n"
               f"Virtuālais konts: {virtual:.2f}€")
        save_state(state)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        notify(f"⚠️ Bota kļūda: {e}")
        raise
