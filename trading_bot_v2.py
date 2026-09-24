"""
mans-bots v2: skenē ~20 tirgus (valūtas, zelts, sudrabs, nafta, indeksi),
izvēlas stiprāko trendu, ko var atļauties ar virtuālo kontu, un atver
vienu "all-in" mikro darījumu ar TP/SL. Palaiž GitHub Actions.
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
VIRTUAL_BALANCE = float(os.environ.get("VIRTUAL_BALANCE") or 50)
TP_EUR = float(os.environ.get("TP_EUR") or 2)
SL_EUR = float(os.environ.get("SL_EUR") or 3)
KILL_EUR = float(os.environ.get("KILL_EUR") or 25)
MIN_SCORE = float(os.environ.get("MIN_SCORE") or 0.15)
MARGIN_USE = 0.8          # cik daudz no virtuālā konta drīkst likt maržā
MAX_COST_PCT = 0.30       # spreds nedrīkst apēst vairāk par 30% no TP
MODE = (os.environ.get("MODE") or "trade").lower()
LABEL = "mans-bots"
STATE_FILE = "bot_state.json"

TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# (nosaukums, iespējamie simboli brokerī, kotācijas valūta)
UNIVERSE = [
    ("EURUSD", ["EURUSD"], "USD"),
    ("GBPUSD", ["GBPUSD"], "USD"),
    ("USDJPY", ["USDJPY"], "JPY"),
    ("AUDUSD", ["AUDUSD"], "USD"),
    ("USDCAD", ["USDCAD"], "CAD"),
    ("USDCHF", ["USDCHF"], "CHF"),
    ("NZDUSD", ["NZDUSD"], "USD"),
    ("EURJPY", ["EURJPY"], "JPY"),
    ("GBPJPY", ["GBPJPY"], "JPY"),
    ("EURGBP", ["EURGBP"], "GBP"),
    ("AUDJPY", ["AUDJPY"], "JPY"),
    ("EURCHF", ["EURCHF"], "CHF"),
    ("ZELTS", ["XAUUSD", "GOLD"], "USD"),
    ("SUDRABS", ["XAGUSD", "SILVER"], "USD"),
    ("NAFTA WTI", ["XTIUSD", "SPOTCRUDE", "USOIL", "WTI"], "USD"),
    ("NAFTA BRENT", ["XBRUSD", "SPOTBRENT", "UKOIL", "BRENT"], "USD"),
    ("US500", ["US500", "SPX500"], "USD"),
    ("NAS100", ["NAS100", "USTEC", "NDX100"], "USD"),
    ("GER40", ["GER40", "DE40", "GER30"], "EUR"),
    ("US30", ["US30", "DJ30", "WS30"], "USD"),
]

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
SUB_SPOTS_REQ, SUB_SPOTS_RES = 2127, 2128
SPOT_EVENT = 2131
TRENDBARS_REQ, TRENDBARS_RES = 2137, 2138
MARGIN_REQ, MARGIN_RES = 2139, 2140
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
    cur = values[0]
    for v in values:
        cur = v * k + cur * (1 - k)
    return cur


def atr(bars, n=14):
    trs = []
    for i in range(1, len(bars)):
        _, hi, lo, _ = bars[i]
        prev_close = bars[i - 1][3]
        trs.append(max(hi - lo, abs(hi - prev_close), abs(lo - prev_close)))
    last = trs[-n:]
    return sum(last) / len(last) if last else 0


class Client:
    def __init__(self, ws):
        self.ws = ws

    async def send(self, ptype, payload):
        await self.ws.send(json.dumps({"clientMsgId": str(uuid.uuid4()),
                                       "payloadType": ptype, "payload": payload}))

    async def request(self, ptype, payload, expect, timeout=25):
        expect = expect if isinstance(expect, tuple) else (expect,)
        await self.send(ptype, payload)
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

    async def spreads(self, aid, sids, wait=5):
        await self.request(SUB_SPOTS_REQ, {"ctidTraderAccountId": aid,
                                           "symbolId": sids}, SUB_SPOTS_RES)
        out, end = {}, time.time() + wait
        while time.time() < end and len(out) < len(sids):
            try:
                msg = json.loads(await asyncio.wait_for(
                    self.ws.recv(), timeout=max(0.1, end - time.time())))
            except asyncio.TimeoutError:
                break
            if msg.get("payloadType") == SPOT_EVENT:
                p = msg.get("payload", {})
                if "bid" in p and "ask" in p:
                    out[int(p["symbolId"])] = (p["ask"] - p["bid"]) / 100000
        return out


def eur_per_quote(q, px):
    try:
        if q == "EUR":
            return 1.0
        if q == "USD":
            return 1 / px["EURUSD"]
        if q == "JPY":
            return 1 / px["EURJPY"]
        if q == "GBP":
            return 1 / px["EURGBP"]
        if q == "CHF":
            return 1 / px["EURCHF"]
        if q == "CAD":
            return 1 / (px["EURUSD"] * px["USDCAD"])
    except KeyError:
        return None
    return None


def find_symbol(names, aliases):
    for a in aliases:
        if a in names:
            return names[a]
    for a in aliases:
        for n, sid in names.items():
            if n.startswith(a):
                return sid
    return None


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
        money_digits = int(trader.get("moneyDigits", 2))
        balance = trader["balance"] / 10 ** money_digits

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

        rec = await c.request(RECONCILE_REQ, {"ctidTraderAccountId": aid}, RECONCILE_RES)
        bot_pos = [p for p in rec.get("position", [])
                   if p.get("tradeData", {}).get("label") == LABEL]

        if MODE != "test" and virtual <= KILL_EUR:
            for p in bot_pos:
                await c.request(CLOSE_POS_REQ, {
                    "ctidTraderAccountId": aid, "positionId": int(p["positionId"]),
                    "volume": int(p["tradeData"]["volume"])}, EXECUTION_EVENT)
            state["stopped"] = True
            save_state(state)
            notify(f"🛑 KILL-SWITCH: virtuālais konts {virtual:.2f}€. Bots apturēts.")
            return

        if MODE != "test" and bot_pos:
            print(f"Jau ir atvērta bota pozīcija ({len(bot_pos)}), gaidu TP/SL.")
            save_state(state)
            return

        # --- Atrod simbolus ---
        syms = await c.request(SYMBOLS_LIST_REQ, {"ctidTraderAccountId": aid},
                               SYMBOLS_LIST_RES)
        names = {s.get("symbolName", "").upper(): int(s["symbolId"])
                 for s in syms.get("symbol", [])}
        found, missing = [], []
        for label, aliases, quote in UNIVERSE:
            sid = find_symbol(names, aliases)
            if sid:
                found.append((label, sid, quote))
            else:
                missing.append(label)

        details = await c.request(SYMBOL_BY_ID_REQ, {
            "ctidTraderAccountId": aid, "symbolId": [sid for _, sid, _ in found]},
            SYMBOL_BY_ID_RES)
        info = {int(s["symbolId"]): s for s in details.get("symbol", [])}

        # --- Skenē tirgus ---
        now_ms = int(time.time() * 1000)
        px, scanned, skipped = {}, [], []
        for label, sid, quote in found:
            await asyncio.sleep(0.3)   # cTrader limits vēsturiskajiem datiem
            tb = await c.request(TRENDBARS_REQ, {
                "ctidTraderAccountId": aid, "symbolId": sid, "period": M5,
                "fromTimestamp": now_ms - 26 * 3600 * 1000, "toTimestamp": now_ms,
            }, TRENDBARS_RES)
            raw = sorted(tb.get("trendbar", []),
                         key=lambda b: b["utcTimestampInMinutes"])[:-1]
            bars = [(b["utcTimestampInMinutes"],
                     (b["low"] + b.get("deltaHigh", 0)) / 100000,
                     b["low"] / 100000,
                     (b["low"] + b.get("deltaClose", 0)) / 100000) for b in raw]
            if len(bars) < 60:
                skipped.append(f"{label}: par maz datu")
                continue
            px[label] = bars[-1][3]
            age = time.time() / 60 - bars[-1][0]
            if age > 20:
                skipped.append(f"{label}: tirgus slēgts")
                continue
            closes = [b[3] for b in bars]
            a = atr(bars)
            if a <= 0:
                skipped.append(f"{label}: nav kustības")
                continue
            diff = ema(closes, 9) - ema(closes, 21)
            score = abs(diff) / a
            if (diff > 0) == (closes[-1] > ema(closes, 50)):
                score *= 1.3          # bonuss, ja sakrīt ar garāku trendu
            scanned.append({"label": label, "sid": sid, "quote": quote,
                            "side": BUY if diff > 0 else SELL, "score": score,
                            "price": closes[-1]})

        # --- Izmērs, marža, TP/SL ---
        candidates = []
        for s in sorted(scanned, key=lambda x: x["score"], reverse=True):
            if s["score"] < MIN_SCORE:
                skipped.append(f"{s['label']}: vājš trends ({s['score']:.2f})")
                continue
            eq = eur_per_quote(s["quote"], px)
            if not eq:
                skipped.append(f"{s['label']}: nav valūtas kursa")
                continue
            d = info.get(s["sid"], {})
            min_vol = int(d.get("minVolume", 100000))
            step = int(d.get("stepVolume", min_vol))
            max_vol = int(d.get("maxVolume", 10 ** 12))
            digits = int(d.get("digits", 5))
            m = await c.request(MARGIN_REQ, {"ctidTraderAccountId": aid,
                                             "symbolId": s["sid"],
                                             "volume": [min_vol]}, MARGIN_RES)
            md = int(m.get("moneyDigits", money_digits))
            mm = (m.get("margin") or [{}])[0]
            margin_min = max(mm.get("buyMargin", 0), mm.get("sellMargin", 0)) / 10 ** md
            budget = virtual * MARGIN_USE
            if margin_min <= 0 or margin_min > budget:
                skipped.append(f"{s['label']}: par dārgu (min marža {margin_min:.0f}€)")
                continue
            volume = int(budget / margin_min * min_vol) // step * step
            volume = max(min_vol, min(volume, max_vol))
            units = volume / 100
            factor = 10 ** (5 - digits)
            tp_rel = max(factor, round(TP_EUR / (units * eq) * 100000 / factor) * factor)
            sl_rel = max(factor, round(SL_EUR / (units * eq) * 100000 / factor) * factor)
            s.update(volume=volume, units=units, eq=eq, tp_rel=tp_rel, sl_rel=sl_rel,
                     margin=margin_min * volume / min_vol)
            candidates.append(s)

        # --- Izmaksu (spreda) pārbaude top 3 ---
        chosen = None
        top = candidates[:3]
        if top:
            spr = await c.spreads(aid, [s["sid"] for s in top])
            for s in top:
                sp = spr.get(s["sid"])
                s["cost"] = sp * s["units"] * s["eq"] if sp is not None else None
                if s["cost"] is None:
                    skipped.append(f"{s['label']}: nav spreda datu")
                elif s["cost"] > TP_EUR * MAX_COST_PCT:
                    skipped.append(f"{s['label']}: par dārgs spreds ({s['cost']:.2f}€)")
                elif chosen is None:
                    chosen = s

        arrow = {BUY: "BUY 📈", SELL: "SELL 📉"}

        if MODE == "test":
            lines = [f"🧪 TESTS OK | virtuālais konts {virtual:.2f}€ "
                     f"(reālais {balance:.2f}€)",
                     f"Atrasti {len(found)}/{len(UNIVERSE)} tirgi"
                     + (f", nav: {', '.join(missing)}" if missing else ""),
                     "", "Top trendi:"]
            for s in sorted(scanned, key=lambda x: x["score"], reverse=True)[:8]:
                lines.append(f"  {s['label']}: {arrow[s['side']]} spēks {s['score']:.2f}")
            if chosen:
                lines += ["", f"👉 Izvēlētos: {chosen['label']} {arrow[chosen['side']]}",
                          f"   lielums {chosen['units']:g}, marža ~{chosen['margin']:.0f}€, "
                          f"spreds ~{chosen['cost']:.2f}€"]
            else:
                lines += ["", "👉 Šobrīd neko neatvērtu."]
            if skipped:
                lines += ["", "Izlaisti:"] + [f"  {x}" for x in skipped[:12]]
            notify("\n".join(lines))
            save_state(state)
            return

        if not chosen:
            print("Nav piemērota tirgus šobrīd. Izlaisti:\n" + "\n".join(skipped))
            save_state(state)
            return

        await c.request(NEW_ORDER_REQ, {
            "ctidTraderAccountId": aid, "symbolId": chosen["sid"],
            "orderType": MARKET, "tradeSide": chosen["side"],
            "volume": chosen["volume"],
            "relativeStopLoss": chosen["sl_rel"],
            "relativeTakeProfit": chosen["tp_rel"],
            "label": LABEL,
        }, EXECUTION_EVENT)
        notify(f"🤖 Atvērts {arrow[chosen['side']]} {chosen['label']}\n"
               f"Cena ~{chosen['price']:g} | lielums {chosen['units']:g}\n"
               f"TP +{TP_EUR:g}€ | SL -{SL_EUR:g}€ | trenda spēks {chosen['score']:.2f}\n"
               f"Virtuālais konts: {virtual:.2f}€")
        save_state(state)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        notify(f"⚠️ Bota kļūda: {e}")
        raise
