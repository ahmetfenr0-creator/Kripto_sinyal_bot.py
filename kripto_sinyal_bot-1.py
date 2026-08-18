"""
Kripto Al-Sat Sinyal Botu (Gelişmiş / Filtreli / Takipli / Etkileşimli Versiyon)
====================================================================================
Binance verisiyle EMA + RSI + MACD + Hacim Patlaması + Trend teyidine dayalı,
SIKI filtreli sinyal üretir. Günün en güçlü sinyallerini gönderir, açık
pozisyonları takip eder (TP/SL vurunca haber verir, TP sonrası SL'i otomatik
kâr korumaya çeker), aşırı volatil coinleri eler, genel piyasa yönüne (BTC)
göre altcoin sinyallerini filtreler, isteğe bağlı haber filtresi uygular ve
Telegram üzerinden /fiyat, /durum, /rapor gibi komutlara canlı cevap verir.

Kurulum:
    pip install ccxt pandas requests

Kullanım:
    python kripto_sinyal_bot.py            -> canlı tarama + takip + komut dinleme
    BACKTEST_MODE = True yapıp çalıştır     -> geçmiş veride temel stratejiyi test eder

ÖNEMLİ / DÜRÜST UYARI:
Hiçbir sistem sabit bir kazanma oranı garanti edemez. BACKTEST_MODE ile
stratejinin geçmişte ne yaptığını görebilirsin, ama bu gelecekteki
performansın garantisi değildir. Kağıt üzerinde (paper trading) test
etmeden gerçek parayla kullanma. Bu bot gerçek işlem AÇMAZ, sadece
sinyal üretir ve fiyatı takip eder - alım/satımı sen (veya borsa
arayüzün) gerçekleştirir.

Bu script çalıştığın klasörde şu dosyaları oluşturur (silmen gerekmez,
silersen geçmiş/kota sıfırlanır):
    positions.json         -> şu an açık takip edilen sinyaller
    signal_log.json         -> gönderilen tüm sinyallerin geçmişi
    daily_count.json        -> bugün kaç sinyal gönderildiği
    report_state.json       -> son gönderilen rapor tarihleri
    telegram_offset.json    -> komut dinleyicinin nerede kaldığı

YENİ ÖZELLİKLER:
1) TRAILING STOP: TP1 vurulunca SL otomatik girişe (breakeven) çekilir,
   TP2 vurulunca SL, TP1 seviyesine çekilir. Böylece kâr güvenceye alınır.
2) KISMİ KÂR ALMA: Bot gerçek emir açmadığı için bu bir YÖNLENDİRMEDİR -
   her TP mesajında "pozisyonun %X'ini kapat" önerisi gelir
   (bkz. PARTIAL_TP_PERCENTAGES).
3) TELEGRAM KOMUTLARI: Botuna yazabilirsin:
   /fiyat BTC   -> anlık fiyat
   /durum       -> açık pozisyonlar
   /rapor       -> anlık günlük özet
   /yardim      -> komut listesi
4) PİYASA REJİMİ FİLTRESİ: BTC güçlü düşüş trendindeyken altcoinlere LONG,
   güçlü yükseliş trendindeyken SHORT sinyali engellenir (MARKET_REGIME_FILTER_ENABLED).
5) HABER FİLTRESİ (opsiyonel/ücretsiz key): CryptoPanic API key girilirse,
   son dakikalarda "important" haber çıkan coinlerde geçici olarak sinyal
   durdurulur. Key girilmezse özellik sessizce pasif kalır.

BALİNA TAKİBİ (önceki sürümden):
- Ücretsiz: Binance genel işlem verisinden büyük alım/satım tespiti.
- Anlık tetikleme: Tek işlemde WHALE_TRIGGER_USD üzeri görülürse coin hemen analiz edilir.
- Opsiyonel/ücretli: whale-alert.io API entegrasyonu (USE_WHALE_ALERT_API).
"""

import time
import json
import os
import threading
import requests
import pandas as pd
import ccxt
from datetime import datetime, timedelta

# ==================== CONFIG ====================

TELEGRAM_BOT_TOKEN = "8988100886:AAFSLUxzWNoL2kpquLbUAv0wsyzaDBTc_MU"
TELEGRAM_CHAT_ID = "6723224182"

SCAN_ALL_COINS = True
SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
QUOTE_CURRENCY = "USDT"
MIN_VOLUME_USDT = 5_000_000
TIMEFRAME = "1h"
TREND_TIMEFRAME = "4h"
MTF_CONFIRM_TIMEFRAME = "15m"
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

MAX_ATR_PERCENT = 8.0
MAX_24H_CHANGE_PERCENT = 25.0

SL_ATR_MULTIPLIER = 1.5
TP_ATR_MULTIPLIERS = [1.5, 3.0, 5.0]

LEVERAGE_MAX = 10
RISK_PER_TRADE_PERCENT = 5.0

MAX_SIGNALS_PER_DAY = 3
WHALE_TRIGGER_MAX_SIGNALS_PER_DAY = 5

WHALE_LARGE_TRADE_USD = 50_000
WHALE_TRIGGER_USD = 100_000
WHALE_LOOKBACK_TRADES = 1000
WHALE_RATIO_THRESHOLD = 1.6
WHALE_SCORE_BONUS = 2.5

USE_WHALE_ALERT_API = False
WHALE_ALERT_API_KEY = "BURAYA_WHALE_ALERT_API_KEY"
WHALE_ALERT_MIN_VALUE_USD = 500_000

# ---- YENİ: Trailing stop ----
TRAILING_STOP_ENABLED = True   # TP1'de SL->giriş, TP2'de SL->TP1

# ---- YENİ: Kısmi kâr alma (bilgilendirme amaçlı öneri) ----
PARTIAL_TP_PERCENTAGES = [50, 30, 20]   # TP1'de %50, TP2'de %30, TP3'te kalan %20

# ---- YENİ: Telegram komutları ----
ENABLE_TELEGRAM_COMMANDS = True
COMMAND_POLL_INTERVAL_SEC = 5
TELEGRAM_OFFSET_FILE = "telegram_offset.json"

# ---- YENİ: Piyasa rejimi (BTC trendine göre altcoin filtresi) ----
MARKET_REGIME_FILTER_ENABLED = True
MARKET_REGIME_SYMBOL = "BTC/USDT"

# ---- YENİ: Haber filtresi (opsiyonel/ücretsiz CryptoPanic API key) ----
USE_NEWS_FILTER = False
CRYPTOPANIC_API_KEY = "BURAYA_CRYPTOPANIC_API_KEY"   # cryptopanic.com/developers/api/keys üzerinden ücretsiz alınır
NEWS_LOOKBACK_MINUTES = 60

RUN_LOOP = True
LOOP_INTERVAL_SEC = 900

BACKTEST_MODE = False
BACKTEST_SYMBOL = "BTC/USDT"
BACKTEST_CANDLES = 1000
BACKTEST_LOOKAHEAD = 48

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


# ---------- YENİ: Piyasa rejimi filtresi ----------

def get_market_regime(exchange):
    """
    BTC'nin kendi trend + momentum durumuna göre genel piyasa yönünü belirler.
    BULLISH: altcoin SHORT sinyalleri elenir. BEARISH: altcoin LONG sinyalleri elenir.
    """
    if not MARKET_REGIME_FILTER_ENABLED:
        return "NEUTRAL"
    try:
        df = fetch_ohlcv(exchange, MARKET_REGIME_SYMBOL, TREND_TIMEFRAME, limit=250)
        df = compute_indicators(df)
        last = df.iloc[-1]
        if last["close"] > last["ema_trend"] and last["macd_hist"] > 0:
            return "BULLISH"
        elif last["close"] < last["ema_trend"] and last["macd_hist"] < 0:
            return "BEARISH"
        return "NEUTRAL"
    except Exception as e:
        print(f"Piyasa rejimi belirlenemedi: {e}")
        return "NEUTRAL"


def passes_market_regime(symbol, direction, market_regime):
    """BTC'nin kendisi için bu filtre uygulanmaz, sadece altcoinler için."""
    if not MARKET_REGIME_FILTER_ENABLED or symbol == MARKET_REGIME_SYMBOL:
        return True
    if market_regime == "BEARISH" and direction == "BUY":
        return False
    if market_regime == "BULLISH" and direction == "SELL":
        return False
    return True


# ---------- YENİ: Haber filtresi (opsiyonel/ücretsiz) ----------

def check_recent_important_news(symbol):
    """
    CryptoPanic ücretsiz API key ile: son NEWS_LOOKBACK_MINUTES içinde bu coin
    için 'important' etiketli haber varsa True döner (sinyal o coin için atlanır).
    Key girilmemişse özellik sessizce pasif kalır (her zaman False döner).
    """
    if not USE_NEWS_FILTER or not CRYPTOPANIC_API_KEY or CRYPTOPANIC_API_KEY.startswith("BURAYA"):
        return False
    try:
        coin_code = symbol.split("/")[0]
        url = "https://cryptopanic.com/api/v1/posts/"
        params = {
            "auth_token": CRYPTOPANIC_API_KEY,
            "currencies": coin_code,
            "filter": "important",
            "public": "true",
        }
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        posts = data.get("results", [])
        cutoff = datetime.utcnow() - timedelta(minutes=NEWS_LOOKBACK_MINUTES)
        for post in posts:
            published = post.get("published_at")
            if not published:
                continue
            post_time = datetime.fromisoformat(published.replace("Z", "+00:00")).replace(tzinfo=None)
            if post_time >= cutoff:
                return True
        return False
    except Exception as e:
        print(f"Haber kontrolü hatası ({symbol}): {e}")
        return False


# ---------- Balina tespiti ----------

def detect_whale_activity(exchange, symbol):
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
    atr_pct = (last_row["atr"] / last_row["close"]) * 100
    if atr_pct > MAX_ATR_PERCENT:
        return False
    change_24h = ticker.get("percentage") if ticker else None
    if change_24h is not None and abs(change_24h) > MAX_24H_CHANGE_PERCENT:
        return False
    return True


def compute_score(volume_ratio, mtf_confirm, rsi, direction):
    score = 4.0
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
    sl_distance_pct = abs(entry_price - sl_price) / entry_price * 100
    if sl_distance_pct <= 0:
        return 1
    leverage = RISK_PER_TRADE_PERCENT / sl_distance_pct
    return max(1, min(LEVERAGE_MAX, round(leverage)))


# ---------- Telegram: mesaj gönderme ----------

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
    tp_lines = "\n".join([
        f"TP{i+1}={tp:.6f} (burada %{PARTIAL_TP_PERCENTAGES[i] if i < len(PARTIAL_TP_PERCENTAGES) else 0} kapat)"
        for i, tp in enumerate(tps)
    ])
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
        f"🔒 TP1 vurunca SL girişe, TP2 vurunca SL TP1'e çekilecek (otomatik).\n"
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
    """
    Açık pozisyonların TP/SL'e ulaşıp ulaşmadığını kontrol eder, Telegram'a
    haber verir. TRAILING_STOP_ENABLED açıksa: TP1 vurulunca SL girişe,
    TP2 vurulunca SL TP1 seviyesine otomatik çekilir (kâr korunur).
    """
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
        entry = pos["entry"]
        tps = pos["tps"]
        hit_tps = pos.get("hit_tps", [])

        hit_sl = (price <= sl) if direction == "BUY" else (price >= sl)
        if hit_sl:
            was_trailed = sl != positions[symbol].get("original_sl", sl)
            note = " (trailing stop ile kâr korunarak kapandı)" if hit_tps else ""
            send_telegram_message(f"🛑 *STOP LOSS* — {symbol}{note}\nFiyat SL seviyesine ulaştı: {price:.6f}")
            update_log_outcome(log, pos["log_id"], "SL" if not hit_tps else f"SL(TP{len(hit_tps)} sonrası)")
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
                partial_pct = PARTIAL_TP_PERCENTAGES[i] if i < len(PARTIAL_TP_PERCENTAGES) else 0
                send_telegram_message(
                    f"✅ *TP{i+1} VURULDU* — {symbol}\nFiyat: {price:.6f}\n"
                    f"💰 Öneri: pozisyonun %{partial_pct}'ini kapat, kalanını sonraki hedefe taşı."
                )

                if TRAILING_STOP_ENABLED and i < len(tps) - 1:
                    new_sl = entry if i == 0 else tps[i - 1]
                    better_for_buy = direction == "BUY" and new_sl > sl
                    better_for_sell = direction == "SELL" and new_sl < sl
                    if better_for_buy or better_for_sell:
                        sl = new_sl
                        positions[symbol]["sl"] = sl
                        send_telegram_message(f"🔒 SL güncellendi — {symbol}\nYeni SL: {sl:.6f} (kâr korumaya alındı)")

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
    sl_count = sum(1 for e in entries if e["outcome"].startswith("SL"))
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


# ---------- YENİ: Telegram komut dinleyici (arka planda çalışır) ----------

def get_telegram_updates(offset=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"timeout": 20}
    if offset:
        params["offset"] = offset
    try:
        r = requests.get(url, params=params, timeout=25)
        return r.json().get("result", [])
    except Exception as e:
        print(f"Telegram getUpdates hatası: {e}")
        return []


def handle_telegram_command(text, exchange):
    text = text.strip()
    lower = text.lower()

    if lower.startswith("/fiyat") or lower.startswith("/price"):
        parts = text.split()
        if len(parts) < 2:
            send_telegram_message("Kullanım: /fiyat BTC")
            return
        coin = parts[1].upper().replace("USDT", "").replace("/", "")
        symbol = f"{coin}/USDT"
        try:
            ticker = exchange.fetch_ticker(symbol)
            change = ticker.get("percentage")
            change_text = f"%{change:.2f}" if change is not None else "bilinmiyor"
            send_telegram_message(f"💰 *{symbol}*\nFiyat: {ticker['last']}\n24s değişim: {change_text}")
        except Exception:
            send_telegram_message(f"{symbol} bulunamadı, sembolü kontrol et (örn: /fiyat BTC).")

    elif lower.startswith("/durum") or lower.startswith("/pozisyonlar"):
        positions = load_json(POSITIONS_FILE, {})
        if not positions:
            send_telegram_message("📂 Şu an açık pozisyon yok.")
            return
        lines = ["📂 *Açık Pozisyonlar*\n"]
        for symbol, pos in positions.items():
            hit_count = len(pos.get("hit_tps", []))
            lines.append(
                f"{symbol} | {pos['direction']} | Giriş: {pos['entry']:.6f} | "
                f"SL: {pos['sl']:.6f} | {hit_count} TP vuruldu"
            )
        send_telegram_message("\n".join(lines))

    elif lower.startswith("/rapor"):
        send_report(days=1, title="Anlık Talep - Günlük Özet")

    elif lower.startswith("/yardim") or lower.startswith("/help") or lower.startswith("/start"):
        send_telegram_message(
            "🤖 *Komutlar*\n\n"
            "/fiyat BTC — coinin anlık fiyatı\n"
            "/durum — açık pozisyonların listesi\n"
            "/rapor — anlık günlük performans özeti\n"
            "/yardim — bu mesaj"
        )


def telegram_command_listener():
    """Ayrı bir arka plan iş parçacığında (thread) sürekli çalışır, botu bloklamaz."""
    exchange = get_exchange()
    state = load_json(TELEGRAM_OFFSET_FILE, {"offset": None})
    while True:
        try:
            updates = get_telegram_updates(offset=state.get("offset"))
            for update in updates:
                state["offset"] = update["update_id"] + 1
                message = update.get("message", {})
                text = message.get("text", "")
                chat_id = str(message.get("chat", {}).get("id", ""))
                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue  # sadece kendi Telegram'ından gelen komutları işle
                if text.startswith("/"):
                    try:
                        handle_telegram_command(text, exchange)
                    except Exception as e:
                        print(f"Komut işleme hatası: {e}")
            save_json(TELEGRAM_OFFSET_FILE, state)
        except Exception as e:
            print(f"Komut dinleyici hatası: {e}")
        time.sleep(COMMAND_POLL_INTERVAL_SEC)


# ---------- Ana tarama ----------

def scan_once():
    exchange = get_exchange()

    check_open_positions(exchange)

    market_regime = get_market_regime(exchange)
    if MARKET_REGIME_FILTER_ENABLED:
        print(f"Piyasa rejimi (BTC bazlı): {market_regime}")

    if SCAN_ALL_COINS:
        tickers = exchange.fetch_tickers()
        symbols_to_scan = get_all_symbols(exchange, tickers)
    else:
        tickers = {}
        symbols_to_scan = SYMBOLS

    positions = load_json(POSITIONS_FILE, {})
    candidates = []
    whale_candidates = []

    for symbol in symbols_to_scan:
        if symbol in positions:
            continue
        try:
            whale = detect_whale_activity(exchange, symbol)
            is_whale_trigger = whale and whale["max_single_trade_usd"] >= WHALE_TRIGGER_USD

            ticker = tickers.get(symbol) if tickers else exchange.fetch_ticker(symbol)

            if is_whale_trigger:
                direction = whale["max_single_trade_direction"]
                if direction is None:
                    continue
                if not passes_market_regime(symbol, direction, market_regime):
                    continue
                if check_recent_important_news(symbol):
                    continue

                trend_direction = get_trend_direction(exchange, symbol)
                df = fetch_ohlcv(exchange, symbol, TIMEFRAME)
                df = compute_indicators(df)
                last = df.iloc[-1]

                if not passes_volatility_filter(last, ticker):
                    continue
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

            trend_direction = get_trend_direction(exchange, symbol)
            df = fetch_ohlcv(exchange, symbol, TIMEFRAME)
            df = compute_indicators(df)
            signal = generate_signal(df, trend_direction=trend_direction)
            if not signal:
                continue

            if not passes_market_regime(symbol, signal, market_regime):
                continue

            last = df.iloc[-1]
            if not passes_volatility_filter(last, ticker):
                continue

            if check_recent_important_news(symbol):
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

    maybe_send_reports()


# ==================== BACKTEST ====================

def backtest_symbol(symbol=BACKTEST_SYMBOL, candles=BACKTEST_CANDLES, lookahead=BACKTEST_LOOKAHEAD):
    """
    Geçmiş veride TEMEL sinyal mantığını (trend+ema+macd+hacim) test eder.
    Not: Çoklu zaman dilimi teyidi, piyasa rejimi, haber filtresi ve trailing
    stop bu testte uygulanmaz, sadece çekirdek stratejinin ham performansını gösterir.
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
    else:
        if ENABLE_TELEGRAM_COMMANDS:
            listener_thread = threading.Thread(target=telegram_command_listener, daemon=True)
            listener_thread.start()
            print("Telegram komut dinleyici başlatıldı (/fiyat, /durum, /rapor, /yardim).")

        if RUN_LOOP:
            while True:
                scan_once()
                time.sleep(LOOP_INTERVAL_SEC)
        else:
            scan_once()
