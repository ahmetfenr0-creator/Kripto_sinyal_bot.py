"""
Kripto Al-Sat Sinyal Botu (Gelişmiş / Filtreli / Takipli Versiyon)
=====================================================================
Binance verisiyle EMA + RSI + MACD + Hacim Patlaması + Trend teyidine
dayalı, SIKI filtreli sinyal üretir. Sadece günün en güçlü 1-3 sinyalini
gönderir, açık pozisyonları takip eder (TP/SL vurunca haber verir),
aşırı volatil/şüpheli coinleri eler ve günlük/haftalık performans
raporu gönderir.

Kurulum:
    pip install ccxt pandas requests

Kullanım:
    python kripto_sinyal_bot.py            -> canlı tarama + takip (Telegram'a gönderir)
    BACKTEST_MODE = True yapıp çalıştır     -> geçmiş veride temel stratejiyi test eder

ÖNEMLİ / DÜRÜST UYARI:
Hiçbir sistem sabit bir kazanma oranı garanti edemez. BACKTEST_MODE ile
stratejinin geçmişte ne yaptığını görebilirsin, ama bu gelecekteki
performansın garantisi değildir. Kağıt üzerinde (paper trading) test
etmeden gerçek parayla kullanma.

Bu script /mnt üzerinde şu dosyaları oluşturur (çalıştığın klasörde):
    positions.json      -> şu an açık takip edilen sinyaller
    signal_log.json      -> gönderilen tüm sinyallerin geçmişi
    daily_count.json     -> bugün kaç sinyal gönderildiği
    report_state.json    -> son gönderilen rapor tarihleri
Bunlar otomatik oluşur, silmen gerekmez; silersen sadece geçmiş sıfırlanır.

BALİNA TAKİBİ NASIL ÇALIŞIYOR:
1) ÜCRETSİZ (varsayılan, otomatik açık): Binance'in kendi son işlem verisinden
   (herkese açık, API key gerekmez) tek seferde WHALE_LARGE_TRADE_USD üzerindeki
   alım/satım emirlerini tarar. Bu, borsada gerçekleşen gerçek büyük emirlerdir -
   zincir üstü cüzdan hareketi değil ama pratikte whale etkisinin borsadaki izidir.
2) ANLIK TETİKLEME: Bir coinde TEK işlemde WHALE_TRIGGER_USD (varsayılan 100.000$)
   üzerinde alım/satım görülürse, bot normal 15dk döngüsünü beklemeden o coini
   hemen analiz eder (trend + RSI + hacim + ATR) ve uygunsa "Coin/Giriş/Kaldıraç/
   TP/SL" formatında anlık sinyal gönderir. Bu sinyaller ayrı bir günlük kotaya
   tabidir (WHALE_TRIGGER_MAX_SIGNALS_PER_DAY).
   ÖNEMLİ: Kaldıraç "coin ne kadar yükselecek" tahminiyle DEĞİL, stop-loss
   mesafesine göre hesaplanır (bkz. suggest_leverage) - bu volatiliteye göre
   otomatik ayarlanan, güvenli ve sorumlu bir yöntemdir.
3) OPSİYONEL/ÜCRETLİ: Whale Alert (whale-alert.io) API'si zincir üstü (cüzdandan
   cüzdana, cüzdandan borsaya) gerçek transferleri verir, aylık ücretli abonelik
   gerektirir. USE_WHALE_ALERT_API=True yapıp WHALE_ALERT_API_KEY'i doldurursan
   devreye girer; doldurmazsan bot yine 1-2. yöntemle (ücretsiz) çalışır.
"""

import time
import json
import os
import requests
import pandas as pd
import ccxt
from datetime import datetime, timedelta

# ==================== CONFIG ====================

TELEGRAM_BOT_TOKEN = "8988100886:AAFSLUxzWNoL2kpquLbUAv0wsyzaDBTc_MU"
TELEGRAM_CHAT_ID = "6723224182"

SCAN_ALL_COINS = True
SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]   # SCAN_ALL_COINS=False ise sadece bu liste taranır
QUOTE_CURRENCY = "USDT"
MIN_VOLUME_USDT = 5_000_000
TIMEFRAME = "1h"
TREND_TIMEFRAME = "4h"
MTF_CONFIRM_TIMEFRAME = "15m"     # Ekstra teyit için üçüncü zaman dilimi
EXCHANGE_ID = "binance"

EMA_FAST = 20
EMA_SLOW = 50
EMA_TREND = 200
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9
ATR_PERIOD = 14

VOLUME_MA_PERIOD = 20
VOLUME_SPIKE_MULTIPLIER = 1.8
MIN_CONFIRMATIONS = 4

# Volatilite / scam-coin filtresi
MAX_ATR_PERCENT = 8.0          # ATR, fiyatın %8'inden fazlaysa coin çok oynak sayılır, elenir
MAX_24H_CHANGE_PERCENT = 25.0  # Son 24 saatte %25'ten fazla hareket etmiş coinler elenir (pump/dump riski)

# Risk yönetimi - ATR bazlı
SL_ATR_MULTIPLIER = 1.5
TP_ATR_MULTIPLIERS = [1.5, 3.0, 5.0]

# Kaldıraç önerisi: "coin ne kadar yükselecek" tahminine göre DEĞİL,
# stop-loss mesafesine (volatiliteye) göre hesaplanır. Mantık: kaldıraç x
# SL mesafesi% = marjinin ne kadarının riske girdiği. Bunu sabit bir üst
# sınırda tutuyoruz ki SL'e gelirse marjinin çok büyük bir kısmı gitmesin.
LEVERAGE_MAX = 10                    # Önerilecek maksimum kaldıraç
RISK_PER_TRADE_PERCENT = 5.0         # SL'e gelirse marjinin en fazla bu yüzdesi kaybedilsin

# Günlük sinyal kotası - sadece en güçlü sinyaller
MAX_SIGNALS_PER_DAY = 3
WHALE_TRIGGER_MAX_SIGNALS_PER_DAY = 5   # Balina tetikli anlık sinyaller için ayrı kota

# ---- Balina (whale) takibi ----
WHALE_LARGE_TRADE_USD = 50_000       # Bu tutarın üstündeki tekil işlemler "balina işlemi" sayılır (skor bonusu için)
WHALE_TRIGGER_USD = 100_000          # Bu tutarın üstünde TEK işlem görülürse coin anında analiz edilir
WHALE_LOOKBACK_TRADES = 1000          # Son kaç işlem taransın (Binance public trade geçmişi)
WHALE_RATIO_THRESHOLD = 1.6           # Büyük alım hacmi, büyük satımın kaç katı olursa "balina teyidi" verilsin
WHALE_SCORE_BONUS = 2.5               # Balina teyidi varsa sinyal skoruna eklenecek bonus

# Opsiyonel/ücretli: whale-alert.io zincir-üstü API entegrasyonu (varsayılan kapalı)
USE_WHALE_ALERT_API = False
WHALE_ALERT_API_KEY = "BURAYA_WHALE_ALERT_API_KEY"
WHALE_ALERT_MIN_VALUE_USD = 500_000

RUN_LOOP = True
LOOP_INTERVAL_SEC = 900

BACKTEST_MODE = False
BACKTEST_SYMBOL = "BTC/USDT"
BACKTEST_CANDLES = 1000
BACKTEST_LOOKAHEAD = 48

# Veri dosyaları
POSITIONS_FILE = "positions.json"
SIGNAL_LOG_FILE = "signal_log.json"
DAILY_COUNT_FILE = "daily_count.json"
REPORT_STATE_FILE = "report_state.json"

# ==================================================


# ---------- Basit JSON depolama ----------

def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------- Borsa & veri ----------

def get_exchange():
    exchange_class = getattr(ccxt, EXCHANGE_ID)
    return exchange_class({"enableRateLimit": True})


def get_all_symbols(exchange, tickers):
    markets = exchange.load_markets()
    symbols = []
    for symbol, market in markets.items():
        if not market.get("active"):
            continue
        if market.get("quote") != QUOTE_CURRENCY:
            continue
        if market.get("type") != "spot":
            continue
        ticker = tickers.get(symbol)
        if not ticker or ticker.get("quoteVolume") is None:
            continue
        if ticker["quoteVolume"] < MIN_VOLUME_USDT:
            continue
        symbols.append(symbol)
    print(f"{len(symbols)} coin taranacak (hacim filtresi: {MIN_VOLUME_USDT:,.0f} {QUOTE_CURRENCY})")
    return symbols


def fetch_ohlcv(exchange, symbol, timeframe, limit=300):
    data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df


def compute_indicators(df):
    df["ema_fast"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    df["ema_trend"] = df["close"].ewm(span=EMA_TREND, adjust=False).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(RSI_PERIOD).mean()
    avg_loss = loss.rolling(RSI_PERIOD).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    ema_fast_macd = df["close"].ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow_macd = df["close"].ewm(span=MACD_SLOW, adjust=False).mean()
    df["macd"] = ema_fast_macd - ema_slow_macd
    df["macd_signal"] = df["macd"].ewm(span=MACD_SIGNAL, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["atr"] = tr.rolling(ATR_PERIOD).mean()

    df["volume_ma"] = df["volume"].rolling(VOLUME_MA_PERIOD).mean()
    df["volume_ratio"] = df["volume"] / df["volume_ma"]

    return df


def get_trend_direction(exchange, symbol):
    df_trend = fetch_ohlcv(exchange, symbol, TREND_TIMEFRAME, limit=250)
    df_trend = compute_indicators(df_trend)
    last = df_trend.iloc[-1]
    return "UP" if last["close"] > last["ema_trend"] else "DOWN"


def check_mtf_confirmation(exchange, symbol, direction):
    """15 dakikalık grafikte de aynı yönde momentum var mı? (Bonus puan için, zorunlu değil)"""
    try:
        df15 = fetch_ohlcv(exchange, symbol, MTF_CONFIRM_TIMEFRAME, limit=100)
        df15 = compute_indicators(df15)
        last15 = df15.iloc[-1]
        if direction == "BUY":
            return last15["ema_fast"] > last15["ema_slow"] and last15["macd_hist"] > 0
        else:
            return last15["ema_fast"] < last15["ema_slow"] and last15["macd_hist"] < 0
    except Exception:
        return False


# ---------- Balina tespiti ----------

def detect_whale_activity(exchange, symbol):
    """
    ÜCRETSİZ yöntem: Binance'in herkese açık son işlem verisinden
    WHALE_LARGE_TRADE_USD üzerindeki tekil alım/satım emirlerini toplar.
    API key gerekmez. Döndürür: buy_usd, sell_usd, direction (BUY/SELL/None), ratio
    """
    try:
        trades = exchange.fetch_trades(symbol, limit=WHALE_LOOKBACK_TRADES)
    except Exception as e:
        print(f"{symbol} balina verisi çekilemedi: {e}")
        return None

    buy_usd = 0.0
    sell_usd = 0.0
    max_single_trade_usd = 0.0
    max_single_trade_direction = None

    for t in trades:
        cost = t.get("cost")
        if cost is None:
            price = t.get("price") or 0
            amount = t.get("amount") or 0
            cost = price * amount
        side = t.get("side")

        if cost >= WHALE_LARGE_TRADE_USD:
            if side == "buy":
                buy_usd += cost
            elif side == "sell":
                sell_usd += cost

        if cost > max_single_trade_usd:
            max_single_trade_usd = cost
            max_single_trade_direction = "BUY" if side == "buy" else ("SELL" if side == "sell" else None)

    if buy_usd == 0 and sell_usd == 0:
        return {
            "buy_usd": 0, "sell_usd": 0, "direction": None, "ratio": 0,
            "max_single_trade_usd": max_single_trade_usd,
            "max_single_trade_direction": max_single_trade_direction,
        }

    direction = None
    if sell_usd == 0 and buy_usd > 0:
        direction = "BUY"
        ratio = float("inf")
    elif buy_usd == 0 and sell_usd > 0:
        direction = "SELL"
        ratio = float("inf")
    else:
        ratio = buy_usd / sell_usd
        if ratio >= WHALE_RATIO_THRESHOLD:
            direction = "BUY"
        elif (1 / ratio) >= WHALE_RATIO_THRESHOLD:
            direction = "SELL"

    return {
        "buy_usd": buy_usd, "sell_usd": sell_usd, "direction": direction, "ratio": ratio,
        "max_single_trade_usd": max_single_trade_usd,
        "max_single_trade_direction": max_single_trade_direction,
    }


def fetch_whale_alert_events(min_value_usd=WHALE_ALERT_MIN_VALUE_USD, minutes=10):
    """
    OPSİYONEL/ÜCRETLİ: whale-alert.io API'si üzerinden son dakikalardaki
    zincir üstü büyük transferleri çeker. Sadece USE_WHALE_ALERT_API=True
    ve geçerli bir WHALE_ALERT_API_KEY varsa çalışır, yoksa boş liste döner.
    """
    if not USE_WHALE_ALERT_API or not WHALE_ALERT_API_KEY or WHALE_ALERT_API_KEY.startswith("BURAYA"):
        return []
    try:
        end = int(time.time())
        start = end - minutes * 60
        url = "https://api.whale-alert.io/v1/transactions"
        params = {
            "api_key": WHALE_ALERT_API_KEY,
            "min_value": min_value_usd,
            "start": start,
            "end": end,
        }
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        return data.get("transactions", [])
    except Exception as e:
        print(f"Whale Alert API hatası: {e}")
        return []


# ---------- Sinyal üretimi ----------

def generate_signal(df, trend_direction=None):
    last = df.iloc[-1]
    prev = df.iloc[-2]

    if trend_direction is None:
        trend_direction = "UP" if last["close"] > last["ema_trend"] else "DOWN"

    ema_cross_up = prev["ema_fast"] <= prev["ema_slow"] and last["ema_fast"] > last["ema_slow"]
    ema_cross_down = prev["ema_fast"] >= prev["ema_slow"] and last["ema_fast"] < last["ema_slow"]

    macd_turn_up = prev["macd_hist"] <= 0 and last["macd_hist"] > 0
    macd_turn_down = prev["macd_hist"] >= 0 and last["macd_hist"] < 0

    volume_spike = last["volume_ratio"] >= VOLUME_SPIKE_MULTIPLIER

    rsi_ok_for_buy = last["rsi"] < RSI_OVERBOUGHT
    rsi_ok_for_sell = last["rsi"] > RSI_OVERSOLD

    buy_checks = [trend_direction == "UP", ema_cross_up, macd_turn_up, volume_spike]
    sell_checks = [trend_direction == "DOWN", ema_cross_down, macd_turn_down, volume_spike]

    if sum(buy_checks) >= MIN_CONFIRMATIONS and rsi_ok_for_buy:
        return "BUY"
    elif sum(sell_checks) >= MIN_CONFIRMATIONS and rsi_ok_for_sell:
        return "SELL"
    return None


def passes_volatility_filter(last_row, ticker):
    """Aşırı oynak veya şüpheli hareket eden coinleri (olası scam/manipülasyon) eler."""
    atr_pct = (last_row["atr"] / last_row["close"]) * 100
    if atr_pct > MAX_ATR_PERCENT:
        return False
    change_24h = ticker.get("percentage") if ticker else None
    if change_24h is not None and abs(change_24h) > MAX_24H_CHANGE_PERCENT:
        return False
    return True


def compute_score(volume_ratio, mtf_confirm, rsi, direction):
    """Sinyalleri sıralamak için kalite puanı. Yüksek puan = daha güçlü teyit."""
    score = 4.0  # temel 4 kriter zaten sağlanmış (generate_signal içinde)
    score += min(volume_ratio, 5.0)
    if mtf_confirm:
        score += 2.0
    if direction == "BUY":
        score += (rsi - 50) / 10
    else:
        score += (50 - rsi) / 10
    return score


def build_levels(entry_price, direction, atr):
    if direction == "BUY":
        sl = entry_price - atr * SL_ATR_MULTIPLIER
        tps = [entry_price + atr * m for m in TP_ATR_MULTIPLIERS]
    else:
        sl = entry_price + atr * SL_ATR_MULTIPLIER
        tps = [entry_price - atr * m for m in TP_ATR_MULTIPLIERS]
    return sl, tps


def suggest_leverage(entry_price, sl_price):
    """
    Kaldıracı 'coin ne kadar yükselecek' tahminine göre DEĞİL, SL mesafesine
    göre hesaplar: SL'e gelinirse marjinin en fazla RISK_PER_TRADE_PERCENT
    kadarı kaybedilsin diye. Volatilitesi yüksek (SL'i uzak) coinlerde
    otomatik olarak DÜŞÜK kaldıraç önerir - bu doğru ve güvenli yöndür.
    """
    sl_distance_pct = abs(entry_price - sl_price) / entry_price * 100
    if sl_distance_pct <= 0:
        return 1
    leverage = RISK_PER_TRADE_PERCENT / sl_distance_pct
    return max(1, min(LEVERAGE_MAX, round(leverage)))


# ---------- Telegram ----------

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code != 200:
            print(f"Telegram gönderim hatası: {r.text}")
    except Exception as e:
        print(f"Telegram bağlantı hatası: {e}")


def format_message(symbol, direction, entry, sl, tps, leverage, rsi, volume_ratio, mtf_confirm, score,
                    whale_note=None, is_whale_trigger=False):
    pos_type = "LONG" if direction == "BUY" else "SHORT"
    header = "🐋 *BALİNA TETİKLİ ANLIK SİNYAL*" if is_whale_trigger else "📡 *SİNYAL*"
    tp_lines = "\n".join([f"TP{i+1}={tp:.6f}" for i, tp in enumerate(tps)])
    mtf_text = "✅ 15dk teyit de var" if mtf_confirm else "➖ 15dk teyidi yok"
    whale_line = f"\n{whale_note}" if whale_note else ""

    msg = (
        f"{header}\n\n"
        f"Coin={symbol}\n"
        f"Yön={pos_type}\n"
        f"Giriş={entry:.6f}\n"
        f"Kaldıraç={leverage}x (izole marj kullan)\n"
        f"{tp_lines}\n"
        f"SL={sl:.6f}\n\n"
        f"📊 RSI: {rsi:.1f} | Hacim: ortalamanın {volume_ratio:.1f}x'i | TF: {TIMEFRAME}\n"
        f"{mtf_text}{whale_line}\n"
        f"⭐ Sinyal skoru: {score:.1f}\n"
        f"⚠️ Kaldıraç SL mesafesine göre hesaplandı, marjinin %{RISK_PER_TRADE_PERCENT:.0f}'i riske girer. "
        f"Yatırım tavsiyesi değildir."
    )
    return msg


# ---------- Günlük kota ----------

def get_daily_state():
    state = load_json(DAILY_COUNT_FILE, {"date": "", "count": 0, "whale_count": 0})
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if state.get("date") != today:
        state = {"date": today, "count": 0, "whale_count": 0}
        save_json(DAILY_COUNT_FILE, state)
    return state


def increment_daily_count():
    state = get_daily_state()
    state["count"] += 1
    save_json(DAILY_COUNT_FILE, state)


def increment_whale_daily_count():
    state = get_daily_state()
    state["whale_count"] += 1
    save_json(DAILY_COUNT_FILE, state)


# ---------- Pozisyon takibi ----------

def open_new_position(symbol, direction, entry, sl, tps):
    positions = load_json(POSITIONS_FILE, {})
    log = load_json(SIGNAL_LOG_FILE, [])

    log_id = f"{symbol}-{datetime.utcnow().isoformat()}"
    log.append({
        "id": log_id, "time": datetime.utcnow().isoformat(), "symbol": symbol,
        "direction": direction, "entry": entry, "sl": sl, "tps": tps,
        "outcome": "OPEN", "closed_time": None,
    })
    positions[symbol] = {
        "direction": direction, "entry": entry, "sl": sl, "tps": tps,
        "hit_tps": [], "log_id": log_id, "opened_time": datetime.utcnow().isoformat(),
    }
    save_json(POSITIONS_FILE, positions)
    save_json(SIGNAL_LOG_FILE, log)


def update_log_outcome(log, log_id, outcome):
    for entry in log:
        if entry["id"] == log_id:
            entry["outcome"] = outcome
            entry["closed_time"] = datetime.utcnow().isoformat()
            break


def check_open_positions(exchange):
    """Açık pozisyonların TP/SL'e ulaşıp ulaşmadığını kontrol eder, Telegram'a haber verir."""
    positions = load_json(POSITIONS_FILE, {})
    if not positions:
        return

    log = load_json(SIGNAL_LOG_FILE, [])
    changed = False

    for symbol, pos in list(positions.items()):
        try:
            ticker = exchange.fetch_ticker(symbol)
            price = ticker["last"]
        except Exception as e:
            print(f"{symbol} fiyat kontrol hatası: {e}")
            continue

        direction = pos["direction"]
        sl = pos["sl"]
        tps = pos["tps"]
        hit_tps = pos.get("hit_tps", [])

        hit_sl = (price <= sl) if direction == "BUY" else (price >= sl)
        if hit_sl:
            send_telegram_message(f"🛑 *STOP LOSS* — {symbol}\nFiyat SL seviyesine ulaştı: {price:.6f}")
            update_log_outcome(log, pos["log_id"], "SL")
            del positions[symbol]
            changed = True
            continue

        position_closed = False
        for i, tp in enumerate(tps):
            if i in hit_tps:
                continue
            hit = (price >= tp) if direction == "BUY" else (price <= tp)
            if hit:
                hit_tps.append(i)
                changed = True
                send_telegram_message(f"✅ *TP{i+1} VURULDU* — {symbol}\nFiyat: {price:.6f}")
                if i == len(tps) - 1:
                    update_log_outcome(log, pos["log_id"], f"TP{i+1}")
                    del positions[symbol]
                    position_closed = True

        if not position_closed and symbol in positions:
            positions[symbol]["hit_tps"] = hit_tps

    if changed:
        save_json(POSITIONS_FILE, positions)
        save_json(SIGNAL_LOG_FILE, log)


# ---------- Performans raporu ----------

def send_report(days, title):
    log = load_json(SIGNAL_LOG_FILE, [])
    cutoff = datetime.utcnow() - timedelta(days=days)
    entries = [e for e in log if datetime.fromisoformat(e["time"]) >= cutoff]

    if not entries:
        send_telegram_message(f"📋 *{title}*\n\nBu dönemde sinyal üretilmedi.")
        return

    total = len(entries)
    tp_count = sum(1 for e in entries if e["outcome"].startswith("TP"))
    sl_count = sum(1 for e in entries if e["outcome"] == "SL")
    open_count = sum(1 for e in entries if e["outcome"] == "OPEN")
    closed = tp_count + sl_count
    win_rate = (tp_count / closed * 100) if closed else 0

    msg = (
        f"📋 *{title}*\n\n"
        f"Toplam sinyal: {total}\n"
        f"✅ TP ile kapanan: {tp_count}\n"
        f"🛑 SL ile kapanan: {sl_count}\n"
        f"⏳ Hâlâ açık: {open_count}\n"
        f"🎯 Kazanma oranı (kapananlar): {win_rate:.1f}%"
    )
    send_telegram_message(msg)


def maybe_send_reports():
    state = load_json(REPORT_STATE_FILE, {"last_daily": "", "last_weekly": ""})
    now = datetime.utcnow()
    today = now.strftime("%Y-%m-%d")

    if state.get("last_daily") != today:
        send_report(days=1, title="Günlük Performans Raporu")
        state["last_daily"] = today
        save_json(REPORT_STATE_FILE, state)

    week_key = now.strftime("%Y-W%W")
    if now.weekday() == 0 and state.get("last_weekly") != week_key:
        send_report(days=7, title="Haftalık Performans Raporu")
        state["last_weekly"] = week_key
        save_json(REPORT_STATE_FILE, state)


# ---------- Ana tarama ----------

def scan_once():
    exchange = get_exchange()

    # 1) Önce açık pozisyonları kontrol et (TP/SL vurdu mu?)
    check_open_positions(exchange)

    # 2) Yeni sinyal adaylarını topla
    if SCAN_ALL_COINS:
        tickers = exchange.fetch_tickers()
        symbols_to_scan = get_all_symbols(exchange, tickers)
    else:
        tickers = {}
        symbols_to_scan = SYMBOLS

    positions = load_json(POSITIONS_FILE, {})
    candidates = []          # normal teknik sinyaller
    whale_candidates = []    # balina tetikli anlık sinyaller (ayrı kota)

    for symbol in symbols_to_scan:
        if symbol in positions:
            continue  # zaten açık pozisyon var, tekrar sinyal üretme
        try:
            # Önce balina kontrolü: tek işlemde WHALE_TRIGGER_USD üzeri var mı?
            whale = detect_whale_activity(exchange, symbol)
            is_whale_trigger = whale and whale["max_single_trade_usd"] >= WHALE_TRIGGER_USD

            ticker = tickers.get(symbol) if tickers else exchange.fetch_ticker(symbol)

            if is_whale_trigger:
                # ---- Balina tetikli anlık analiz: normal 4/4 teyit şartını beklemeden ----
                direction = whale["max_single_trade_direction"]
                if direction is None:
                    continue

                trend_direction = get_trend_direction(exchange, symbol)
                df = fetch_ohlcv(exchange, symbol, TIMEFRAME)
                df = compute_indicators(df)
                last = df.iloc[-1]

                if not passes_volatility_filter(last, ticker):
                    continue
                # Aşırı uçta (zaten tepe/dip yapmış) coine sürüklenerek girmeyi engelle
                if direction == "BUY" and last["rsi"] > 80:
                    continue
                if direction == "SELL" and last["rsi"] < 20:
                    continue

                mtf_confirm = check_mtf_confirmation(exchange, symbol, direction)
                entry = last["close"]
                sl, tps = build_levels(entry, direction, last["atr"])
                leverage = suggest_leverage(entry, sl)

                trend_bonus = 1.0 if (
                    (direction == "BUY" and trend_direction == "UP") or
                    (direction == "SELL" and trend_direction == "DOWN")
                ) else 0.0
                score = 5.0 + trend_bonus + (2.0 if mtf_confirm else 0.0) + min(last["volume_ratio"], 3.0)

                action = "alım" if direction == "BUY" else "satım"
                whale_note = f"🐋 Tek işlemde ${whale['max_single_trade_usd']:,.0f} büyük {action} tespit edildi, anlık analiz tetiklendi"

                whale_candidates.append({
                    "score": score, "symbol": symbol, "direction": direction,
                    "entry": entry, "sl": sl, "tps": tps, "leverage": leverage,
                    "rsi": last["rsi"], "volume_ratio": last["volume_ratio"],
                    "mtf_confirm": mtf_confirm, "whale_note": whale_note,
                })
                continue

            # ---- Normal teknik sinyal (4/4 sıkı teyit) ----
            trend_direction = get_trend_direction(exchange, symbol)
            df = fetch_ohlcv(exchange, symbol, TIMEFRAME)
            df = compute_indicators(df)
            signal = generate_signal(df, trend_direction=trend_direction)
            if not signal:
                continue

            last = df.iloc[-1]
            if not passes_volatility_filter(last, ticker):
                continue

            mtf_confirm = check_mtf_confirmation(exchange, symbol, signal)
            entry = last["close"]
            sl, tps = build_levels(entry, signal, last["atr"])
            leverage = suggest_leverage(entry, sl)
            score = compute_score(last["volume_ratio"], mtf_confirm, last["rsi"], signal)

            whale_note = None
            if whale and whale["direction"] == signal:
                score += WHALE_SCORE_BONUS
                amount = whale["buy_usd"] if signal == "BUY" else whale["sell_usd"]
                action = "alım" if signal == "BUY" else "satım"
                whale_note = f"🐋 Balina teyidi: son işlemlerde ${amount:,.0f} büyük {action} tespit edildi"

            candidates.append({
                "score": score, "symbol": symbol, "direction": signal,
                "entry": entry, "sl": sl, "tps": tps, "leverage": leverage,
                "rsi": last["rsi"], "volume_ratio": last["volume_ratio"],
                "mtf_confirm": mtf_confirm, "whale_note": whale_note,
            })
        except Exception as e:
            print(f"{symbol} taranırken hata: {e}")

    # 3) Önce balina tetikli sinyalleri gönder (kendi kotası var, önceliklidir)
    whale_candidates.sort(key=lambda c: c["score"], reverse=True)
    whale_remaining = WHALE_TRIGGER_MAX_SIGNALS_PER_DAY - get_daily_state()["whale_count"]
    whale_sent = 0
    for c in whale_candidates:
        if whale_sent >= whale_remaining:
            break
        msg = format_message(
            c["symbol"], c["direction"], c["entry"], c["sl"], c["tps"], c["leverage"],
            c["rsi"], c["volume_ratio"], c["mtf_confirm"], c["score"], c["whale_note"],
            is_whale_trigger=True,
        )
        print(msg)
        send_telegram_message(msg)
        open_new_position(c["symbol"], c["direction"], c["entry"], c["sl"], c["tps"])
        increment_whale_daily_count()
        whale_sent += 1

    # 4) Sonra normal teknik sinyallerden en güçlü olanları gönder
    candidates.sort(key=lambda c: c["score"], reverse=True)
    remaining_quota = MAX_SIGNALS_PER_DAY - get_daily_state()["count"]

    sent = 0
    for c in candidates:
        if sent >= remaining_quota:
            break
        msg = format_message(
            c["symbol"], c["direction"], c["entry"], c["sl"], c["tps"], c["leverage"],
            c["rsi"], c["volume_ratio"], c["mtf_confirm"], c["score"], c["whale_note"],
            is_whale_trigger=False,
        )
        print(msg)
        send_telegram_message(msg)
        open_new_position(c["symbol"], c["direction"], c["entry"], c["sl"], c["tps"])
        increment_daily_count()
        sent += 1

    print(
        f"Tarama bitti. {len(whale_candidates)} balina tetikli aday ({whale_sent} gönderildi), "
        f"{len(candidates)} normal aday ({sent} gönderildi)."
    )


    # 4) Günlük/haftalık rapor zamanı geldi mi kontrol et
    maybe_send_reports()


# ==================== BACKTEST ====================

def backtest_symbol(symbol=BACKTEST_SYMBOL, candles=BACKTEST_CANDLES, lookahead=BACKTEST_LOOKAHEAD):
    """
    Geçmiş veride TEMEL sinyal mantığını (trend+ema+macd+hacim) test eder.
    Not: Çoklu zaman dilimi teyidi ve günlük kota bu testte uygulanmaz,
    sadece çekirdek stratejinin ham performansını gösterir.
    """
    exchange = get_exchange()
    df = fetch_ohlcv(exchange, symbol, TIMEFRAME, limit=candles)
    df = compute_indicators(df)

    results = []
    for i in range(EMA_TREND + 10, len(df) - lookahead):
        window = df.iloc[: i + 1]
        last = window.iloc[-1]
        trend_direction = "UP" if last["close"] > last["ema_trend"] else "DOWN"
        signal = generate_signal(window, trend_direction=trend_direction)
        if not signal:
            continue

        entry = last["close"]
        sl, tps = build_levels(entry, signal, last["atr"])
        future = df.iloc[i + 1 : i + 1 + lookahead]

        outcome = "NONE"
        for _, row in future.iterrows():
            if signal == "BUY":
                if row["low"] <= sl:
                    outcome = "SL"
                    break
                hit_tps = [j for j, tp in enumerate(tps) if row["high"] >= tp]
                if hit_tps:
                    outcome = f"TP{max(hit_tps) + 1}"
            else:
                if row["high"] >= sl:
                    outcome = "SL"
                    break
                hit_tps = [j for j, tp in enumerate(tps) if row["low"] <= tp]
                if hit_tps:
                    outcome = f"TP{max(hit_tps) + 1}"

        results.append({"time": last["timestamp"], "signal": signal, "outcome": outcome})

    if not results:
        print("Bu dönemde hiç sinyal üretilmedi. Filtreler çok sıkı olabilir.")
        return

    res_df = pd.DataFrame(results)
    total = len(res_df)
    sl_count = (res_df["outcome"] == "SL").sum()
    none_count = (res_df["outcome"] == "NONE").sum()
    tp_count = total - sl_count - none_count

    print(f"\n===== BACKTEST SONUCU: {symbol} | {TIMEFRAME} | son {candles} mum =====")
    print(f"Toplam sinyal: {total}")
    print(f"TP'ye ulaşan: {tp_count} ({tp_count/total*100:.1f}%)")
    print(f"SL'e takılan: {sl_count} ({sl_count/total*100:.1f}%)")
    print(f"Sonuçlanmayan (süre doldu): {none_count} ({none_count/total*100:.1f}%)")
    print("\nTP dağılımı:")
    print(res_df[res_df["outcome"].str.startswith("TP")]["outcome"].value_counts())
    print("\nNOT: Bu sonuç geçmiş veriye dayanır, gelecekte aynı oran tekrar etmeyebilir.")


if __name__ == "__main__":
    if BACKTEST_MODE:
        backtest_symbol()
    elif RUN_LOOP:
        while True:
            scan_once()
            time.sleep(LOOP_INTERVAL_SEC)
    else:
        scan_once()
