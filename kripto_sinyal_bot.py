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
    pip install ccxt pandas requests websocket-client

YENİ (bu sürüm):
- BOT ARTIK SADECE ALTCOIN SİNYALİ ÜRETİR: BTC ve ETH, EXCLUDE_FROM_SIGNALS
  listesiyle sinyal üretiminden tamamen çıkarıldı (ana tarama, balina
  tetikleme, anlık hacim izleyici - hepsinde). BTC verisi yalnızca piyasa
  yönü filtresi (market regime) için arka planda okunmaya devam eder,
  kendisine asla LONG/SHORT sinyali gelmez. Listeye başka coin eklemek/
  çıkarmak istersen EXCLUDE_FROM_SIGNALS'ı düzenle.
- Güven Puanı (%): her sinyalde artık sabit skor yerine %60-98 arası bir
  güven puanı gösterilir. Kaldıraç da buna göre ayarlanır - düşük güvenli
  sinyalde otomatik düşük kaldıraç, yüksek güvenli sinyalde (ör. %95+)
  LEVERAGE_MAX'e kadar çıkabilir. Risk (SL mesafesi) sınırı her zaman
  öncelikli kalır - ikisinden hangisi daha düşükse o kullanılır.
- Günlük kotalar artırıldı: normal sinyal 3->8, balina tetikli 5->10.
- Ana taramada hacim tabanı 5M->2M USDT'ye düşürüldü (daha fazla altcoin).
- YENİ KATMAN - Anlık Hacim Patlaması İzleyici: Binance WebSocket akışını
  (tüm coinler, saniyede bir, limitsiz) dinleyerek 15-30sn/1dk içinde ani
  hacim+fiyat hareketi olan coinleri ANINDA tespit edip sinyal gönderir,
  15 dakikalık ana döngüyü beklemez. SURGE_MIN_VOLUME_USDT (500k) ile
  düşük hacimli coinler için ayrı, daha gevşek bir taban kullanılır.
  Bu özellik 'websocket-client' kütüphanesi gerektirir (requirements.txt'e ekle).

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

GERÇEK OTOMATİK İŞLEM (YENİ, varsayılan KAPALI):
ENABLE_AUTO_TRADING=True yapıp BINANCE_API_KEY/SECRET doldurursan, bot artık
sadece mesaj göndermekle kalmaz, Binance Futures'ta GERÇEK emir açar/kapatır.
- USE_TESTNET=True (varsayılan): Binance Futures Testnet'te sahte parayla
  çalışır, sıfır risk. Test hesabı ve API key'i testnet.binancefuture.com'dan
  alınır, gerçek Binance hesabınla hiçbir ilgisi yoktur.
- Kaldıraç ve marjin modu (isolated) otomatik ayarlanır, pozisyon büyüklüğü
  bakiyenin POSITION_SIZE_PERCENT kadarı (varsayılan %2) ile hesaplanır.
- SL borsaya gerçek STOP_MARKET emri olarak konur. TP vurulunca pozisyonun
  ilgili yüzdesi gerçek market emriyle kapatılır. Trailing stop tetiklenince
  eski SL emri iptal edilip yenisi konur.
- MAX_CONCURRENT_AUTO_TRADES ile aynı anda kaç gerçek pozisyon açık
  olabileceği sınırlanır.
UYARI: BURAYA_ yazan alanları doldurmadan hiçbir gerçek emir açılmaz.
Gerçek hesapla (USE_TESTNET=False) kullanmadan önce mutlaka testnette
günlerce test et ve API key'e SADECE "Futures" ve "İşlem" izni ver,
"Para Çekme" (withdrawal) iznini KESİNLİKLE açma.

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

try:
    import websocket   # pip install websocket-client (requirements.txt'e eklenmeli)
except ImportError:
    websocket = None

# ==================== CONFIG ====================

TELEGRAM_BOT_TOKEN = "8988100886:AAFSLUxzWNoL2kpquLbUAv0wsyzaDBTc_MU"
TELEGRAM_CHAT_ID = "6723224182"

SCAN_ALL_COINS = True
SYMBOLS = ["SOL/USDT", "BNB/USDT", "XRP/USDT"]   # SCAN_ALL_COINS=False ise sadece bu liste taranır
EXCLUDE_FROM_SIGNALS = ["BTC/USDT", "ETH/USDT"]  # Bot artık SADECE altcoin sinyali üretir, bunlara sinyal atmaz
                                                   # (BTC yine de piyasa yönü filtresi için arka planda kullanılır)
QUOTE_CURRENCY = "USDT"
MIN_VOLUME_USDT = 2_000_000   # Ana teknik tarama için (daha fazla altcoin dahil olsun diye 5M'den düşürüldü)
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

MAX_SIGNALS_PER_DAY = 8              # 3'ten yükseltildi
WHALE_TRIGGER_MAX_SIGNALS_PER_DAY = 10   # 5'ten yükseltildi

# ---- YENİ: Anlık hacim patlaması izleyici (WebSocket, 15-30sn/1dk çözünürlük) ----
# Normal REST taraması 15 dakikada bir çalışır - yüzlerce coini saniyeler
# içinde taramak REST ile mümkün değil (hız limiti). Bunun yerine Binance'in
# canlı WebSocket akışını (tüm coinler için saniyede bir güncellenen veri,
# limitsiz) dinleyerek ANLIK hacim/fiyat patlamalarını yakalıyoruz.
ENABLE_SURGE_WATCHER = True
SURGE_MIN_VOLUME_USDT = 500_000        # Bu 24s hacmin altındaki coinler değerlendirilmez (tam çöp/scam filtre)
SURGE_WINDOW_SECONDS = 60              # Kaç saniyelik pencerede hacim artışı ölçülsün
SURGE_CHECK_INTERVAL_SECONDS = 15      # Pencere kaç saniyede bir kontrol edilsin
SURGE_VOLUME_USD_THRESHOLD = 50_000    # Bu pencerede en az bu kadar $ hacim artışı olmalı
SURGE_PRICE_MOVE_THRESHOLD_PERCENT = 1.5  # Yönü teyit etmek için en az bu kadar fiyat hareketi olmalı
SURGE_COOLDOWN_MINUTES = 30            # Aynı coin için tekrar uyarı vermeden önce bekleme süresi
SURGE_ALERT_MAX_PER_DAY = 15           # Anlık uyarılar için ayrı günlük kota

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
MENU_COINS = ["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE"]   # Tuş menüsünde görünecek coinler
COMMAND_POLL_INTERVAL_SEC = 5
TELEGRAM_OFFSET_FILE = "telegram_offset.json"

# ---- YENİ: Piyasa rejimi (BTC trendine göre altcoin filtresi) ----
MARKET_REGIME_FILTER_ENABLED = True
MARKET_REGIME_SYMBOL = "BTC/USDT"

# ---- YENİ: Haber filtresi (opsiyonel/ücretsiz CryptoPanic API key) ----
USE_NEWS_FILTER = False
CRYPTOPANIC_API_KEY = "BURAYA_CRYPTOPANIC_API_KEY"   # cryptopanic.com/developers/api/keys üzerinden ücretsiz alınır
NEWS_LOOKBACK_MINUTES = 60

# ---- YENİ: Gerçek otomatik işlem (Binance Futures) ----
# UYARI: ENABLE_AUTO_TRADING=True yaptığın anda bot GERÇEK EMİR açar/kapatır.
# Mutlaka önce USE_TESTNET=True ile test et. Bu satırları doldurmadan
# (BURAYA_ yazan yerler) hiçbir gerçek emir açılmaz, bot eskisi gibi
# sadece sinyal/mesaj modunda çalışmaya devam eder.
ENABLE_AUTO_TRADING = False
USE_TESTNET = True
BINANCE_API_KEY = "BURAYA_BINANCE_API_KEY"
BINANCE_API_SECRET = "BURAYA_BINANCE_API_SECRET"
POSITION_SIZE_PERCENT = 2.0     # Kullanılabilir bakiyenin (marjin) yüzde kaçı her işlemde kullanılsın
MARGIN_MODE = "isolated"        # isolated önerilir - cross tüm bakiyeyi tek işlemde riske atar
MAX_CONCURRENT_AUTO_TRADES = 3  # Aynı anda en fazla kaç otomatik pozisyon açık olsun

RUN_LOOP = True
LOOP_INTERVAL_SEC = 900

BACKTEST_MODE = False
BACKTEST_SYMBOL = "SOL/USDT"   # Bot artık altcoin odaklı olduğu için varsayılan test coini değiştirildi
BACKTEST_CANDLES = 1000
BACKTEST_LOOKAHEAD = 48

POSITIONS_FILE = "positions.json"
SIGNAL_LOG_FILE = "signal_log.json"
DAILY_COUNT_FILE = "daily_count.json"
REPORT_STATE_FILE = "report_state.json"
SURGE_STATE_FILE = "surge_cooldown.json"

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
        if symbol in EXCLUDE_FROM_SIGNALS:
            continue  # bot sadece altcoin sinyali üretir
        ticker = tickers.get(symbol)
        if not ticker or ticker.get("quoteVolume") is None:
            continue
        if ticker["quoteVolume"] < MIN_VOLUME_USDT:
            continue
        symbols.append(symbol)
    print(f"{len(symbols)} altcoin taranacak (BTC/ETH hariç, hacim filtresi: {MIN_VOLUME_USDT:,.0f} {QUOTE_CURRENCY})")
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


def compute_confidence_percent(mtf_confirm, whale_aligned, volume_ratio, rsi, direction, is_whale_trigger=False):
    """
    0-100 arası güven puanı üretir. Taban puan, sinyalin zaten geçmesi
    zorunlu olan kriterlere göre başlar (4/4 teknik teyit ya da balina
    tetiklemesi), ekstra teyitler (15dk, balina, güçlü hacim, sağlıklı RSI)
    puanı artırır. %98 üzerine hiç çıkmaz - hiçbir sinyal %100 garanti değildir.
    """
    confidence = 60.0 if is_whale_trigger else 65.0

    volume_bonus = max((volume_ratio - VOLUME_SPIKE_MULTIPLIER) * 5, 0)
    confidence += min(volume_bonus, 15)

    if mtf_confirm:
        confidence += 10

    if whale_aligned:
        confidence += 8

    if direction == "BUY":
        rsi_bonus = max(0, min((rsi - 50) / 3, 7))
    else:
        rsi_bonus = max(0, min((50 - rsi) / 3, 7))
    confidence += rsi_bonus

    return round(min(confidence, 98), 1)


def confidence_leverage_cap(confidence):
    """Güven puanına göre kaldıraç üst sınırı - düşük güven = düşük kaldıraç tavanı."""
    if confidence >= 95:
        return LEVERAGE_MAX
    elif confidence >= 90:
        return min(LEVERAGE_MAX, 8)
    elif confidence >= 80:
        return min(LEVERAGE_MAX, 6)
    elif confidence >= 70:
        return min(LEVERAGE_MAX, 4)
    else:
        return min(LEVERAGE_MAX, 2)


def suggest_leverage(entry_price, sl_price, confidence=70.0):
    """
    Kaldıraç iki sınırdan HANGİSİ DAHA DÜŞÜKSE ona göre belirlenir:
    1) Risk sınırı: SL mesafesine göre (marjinin en fazla %X'i risk altında olsun)
    2) Güven sınırı: sinyalin teyit gücüne göre (düşük güven = düşük tavan)
    Bu sayede kaldıraç ne sadece volatiliteye ne de sadece "iyimserliğe"
    göre değil, ikisinin de en temkinli tarafına göre belirlenir.
    """
    sl_distance_pct = abs(entry_price - sl_price) / entry_price * 100
    if sl_distance_pct <= 0:
        return 1
    risk_based_leverage = RISK_PER_TRADE_PERCENT / sl_distance_pct
    conf_cap = confidence_leverage_cap(confidence)
    leverage = min(risk_based_leverage, conf_cap)
    return max(1, min(LEVERAGE_MAX, round(leverage)))


# ---------- Telegram: mesaj gönderme ----------

def build_main_keyboard():
    """Telegram'ın altında sabit görünen tuşlanabilir menü (reply keyboard)."""
    coin_buttons = [f"💰 {c}" for c in MENU_COINS]
    rows = [coin_buttons[i:i + 3] for i in range(0, len(coin_buttons), 3)]
    rows.append(["📂 Durum", "📋 Rapor"])
    rows.append(["❓ Yardım"])
    return {"keyboard": rows, "resize_keyboard": True, "is_persistent": True}


def send_telegram_message(text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    try:
        r = requests.post(url, data=payload, timeout=10)
        if r.status_code != 200:
            print(f"Telegram gönderim hatası: {r.text}")
    except Exception as e:
        print(f"Telegram bağlantı hatası: {e}")


def format_message(symbol, direction, entry, sl, tps, leverage, rsi, volume_ratio, mtf_confirm, confidence,
                    whale_note=None, is_whale_trigger=False, header_override=None, extra_line=None):
    pos_type = "LONG" if direction == "BUY" else "SHORT"
    if header_override:
        header = header_override
    else:
        header = "🐋 *BALİNA TETİKLİ ANLIK SİNYAL*" if is_whale_trigger else "📡 *SİNYAL*"
    tp_lines = "\n".join([
        f"TP{i+1}={tp:.6f} (burada %{PARTIAL_TP_PERCENTAGES[i] if i < len(PARTIAL_TP_PERCENTAGES) else 0} kapat)"
        for i, tp in enumerate(tps)
    ])
    mtf_text = "✅ 15dk teyit de var" if mtf_confirm else "➖ 15dk teyidi yok"
    whale_line = f"\n{whale_note}" if whale_note else ""
    extra = f"\n{extra_line}" if extra_line else ""

    msg = (
        f"{header}\n\n"
        f"Coin={symbol}\n"
        f"Yön={pos_type}\n"
        f"Giriş={entry:.6f}\n"
        f"Kaldıraç={leverage}x (izole marj kullan)\n"
        f"{tp_lines}\n"
        f"SL={sl:.6f}\n\n"
        f"📊 RSI: {rsi:.1f} | Hacim: ortalamanın {volume_ratio:.1f}x'i | TF: {TIMEFRAME}\n"
        f"{mtf_text}{whale_line}{extra}\n"
        f"🎯 Güven Puanı: %{confidence:.0f}\n"
        f"🔒 TP1 vurunca SL girişe, TP2 vurunca SL TP1'e çekilecek (otomatik).\n"
        f"⚠️ Kaldıraç güven puanı + SL mesafesine göre hesaplandı, marjinin %{RISK_PER_TRADE_PERCENT:.0f}'i riske girer. "
        f"Yatırım tavsiyesi değildir."
    )
    return msg


# ---------- Günlük kota ----------

def get_daily_state():
    state = load_json(DAILY_COUNT_FILE, {"date": "", "count": 0, "whale_count": 0, "surge_count": 0})
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if state.get("date") != today:
        state = {"date": today, "count": 0, "whale_count": 0, "surge_count": 0}
        save_json(DAILY_COUNT_FILE, state)
    if "surge_count" not in state:
        state["surge_count"] = 0
    return state


def increment_daily_count():
    state = get_daily_state()
    state["count"] += 1
    save_json(DAILY_COUNT_FILE, state)


def increment_whale_daily_count():
    state = get_daily_state()
    state["whale_count"] += 1
    save_json(DAILY_COUNT_FILE, state)


def increment_surge_daily_count():
    state = get_daily_state()
    state["surge_count"] += 1
    save_json(DAILY_COUNT_FILE, state)


# ---------- YENİ: Gerçek Binance Futures işlemleri ----------

def get_trading_exchange():
    """
    Gerçek emir açıp kapatmak için kullanılan, API key gerektiren bağlantı.
    ENABLE_AUTO_TRADING=False veya key doldurulmamışsa None döner - bot
    o zaman eskisi gibi sadece sinyal/mesaj modunda çalışır.
    """
    if not ENABLE_AUTO_TRADING:
        return None
    if not BINANCE_API_KEY or BINANCE_API_KEY.startswith("BURAYA") or \
       not BINANCE_API_SECRET or BINANCE_API_SECRET.startswith("BURAYA"):
        print("UYARI: ENABLE_AUTO_TRADING=True ama API key/secret girilmemiş. Gerçek emir açılmayacak.")
        return None
    try:
        exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
        })
        if USE_TESTNET:
            exchange.set_sandbox_mode(True)
        exchange.load_markets()
        return exchange
    except Exception as e:
        send_telegram_message(f"❌ Borsa bağlantı hatası (otomatik işlem): {e}")
        print(f"Trading exchange bağlantı hatası: {e}")
        return None


def count_open_auto_trades():
    positions = load_json(POSITIONS_FILE, {})
    return sum(1 for p in positions.values() if p.get("auto_trade"))


def open_real_position(trading_exchange, symbol, direction, entry, sl, leverage):
    """
    GERÇEK emir açar: kaldıraç + marjin modu ayarlar, market emriyle pozisyonu
    açar, SL için borsaya STOP_MARKET (reduceOnly) emri koyar. TP'ler ise
    check_open_positions döngüsü fiyatı takip ederken kısmi market emirleriyle
    kapatılır (trailing stop mantığıyla uyumlu kalması için).
    """
    try:
        trading_exchange.set_leverage(leverage, symbol)
        try:
            trading_exchange.set_margin_mode(MARGIN_MODE, symbol)
        except Exception:
            pass  # bazı hesaplarda zaten ayarlıysa hata verebilir, yok sayılabilir

        balance = trading_exchange.fetch_balance()
        usdt_free = balance.get("USDT", {}).get("free", 0) or 0
        if usdt_free <= 0:
            send_telegram_message(f"❌ {symbol}: Kullanılabilir USDT bakiyesi yok, emir açılmadı.")
            return None

        margin_to_use = usdt_free * (POSITION_SIZE_PERCENT / 100)
        position_value = margin_to_use * leverage
        amount = position_value / entry
        amount = float(trading_exchange.amount_to_precision(symbol, amount))
        if amount <= 0:
            send_telegram_message(f"❌ {symbol}: Hesaplanan miktar çok küçük, emir açılmadı.")
            return None

        side = "buy" if direction == "BUY" else "sell"
        order = trading_exchange.create_order(symbol, "market", side, amount)

        sl_side = "sell" if direction == "BUY" else "buy"
        sl_order = trading_exchange.create_order(
            symbol, "STOP_MARKET", sl_side, amount, None,
            {"stopPrice": sl, "reduceOnly": True},
        )

        send_telegram_message(
            f"✅ *GERÇEK EMİR AÇILDI* {'(TESTNET)' if USE_TESTNET else ''} — {symbol}\n"
            f"Miktar: {amount}\nKullanılan marjin: ~{margin_to_use:.2f} USDT\nKaldıraç: {leverage}x\n"
            f"SL emri borsaya kondu: {sl:.6f}"
        )

        return {"amount": amount, "sl_order_id": sl_order.get("id"), "margin_used": margin_to_use}
    except Exception as e:
        send_telegram_message(f"❌ Gerçek emir hatası — {symbol}: {e}")
        print(f"{symbol} emir açma hatası: {e}")
        return None


def close_partial_position(trading_exchange, symbol, direction, amount_to_close):
    """TP vurulduğunda pozisyonun bir kısmını GERÇEKTEN market emriyle kapatır."""
    try:
        amount_to_close = float(trading_exchange.amount_to_precision(symbol, amount_to_close))
        if amount_to_close <= 0:
            return
        side = "sell" if direction == "BUY" else "buy"
        trading_exchange.create_order(symbol, "market", side, amount_to_close, None, {"reduceOnly": True})
    except Exception as e:
        send_telegram_message(f"❌ Kısmi kapama hatası — {symbol}: {e}")
        print(f"{symbol} kısmi kapama hatası: {e}")


def close_full_position(trading_exchange, symbol, direction, amount):
    """Kalan tüm pozisyonu GERÇEKTEN market emriyle kapatır (son TP veya manuel kapama için)."""
    try:
        amount = float(trading_exchange.amount_to_precision(symbol, amount))
        if amount <= 0:
            return
        side = "sell" if direction == "BUY" else "buy"
        trading_exchange.create_order(symbol, "market", side, amount, None, {"reduceOnly": True})
    except Exception as e:
        send_telegram_message(f"❌ Pozisyon kapama hatası — {symbol}: {e}")
        print(f"{symbol} tam kapama hatası: {e}")


def update_real_stop_loss(trading_exchange, symbol, direction, remaining_amount, new_sl, old_order_id):
    """Trailing stop tetiklenince eski SL emrini iptal edip yeni fiyattan tekrar koyar."""
    try:
        if old_order_id:
            try:
                trading_exchange.cancel_order(old_order_id, symbol)
            except Exception:
                pass
        sl_side = "sell" if direction == "BUY" else "buy"
        new_order = trading_exchange.create_order(
            symbol, "STOP_MARKET", sl_side, remaining_amount, None,
            {"stopPrice": new_sl, "reduceOnly": True},
        )
        return new_order.get("id")
    except Exception as e:
        send_telegram_message(f"❌ SL güncelleme hatası (gerçek emir) — {symbol}: {e}")
        print(f"{symbol} SL güncelleme hatası: {e}")
        return old_order_id


# ---------- Pozisyon takibi ----------

def open_new_position(symbol, direction, entry, sl, tps, auto_trade_info=None):
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
        "auto_trade": auto_trade_info is not None,
        "amount": auto_trade_info["amount"] if auto_trade_info else None,
        "remaining_amount": auto_trade_info["amount"] if auto_trade_info else None,
        "sl_order_id": auto_trade_info["sl_order_id"] if auto_trade_info else None,
    }
    save_json(POSITIONS_FILE, positions)
    save_json(SIGNAL_LOG_FILE, log)


def update_log_outcome(log, log_id, outcome):
    for entry in log:
        if entry["id"] == log_id:
            entry["outcome"] = outcome
            entry["closed_time"] = datetime.utcnow().isoformat()
            break


def check_open_positions(exchange, trading_exchange=None):
    """
    Açık pozisyonların TP/SL'e ulaşıp ulaşmadığını kontrol eder, Telegram'a
    haber verir. TRAILING_STOP_ENABLED açıksa: TP1 vurulunca SL girişe,
    TP2 vurulunca SL TP1 seviyesine otomatik çekilir (kâr korunur).
    trading_exchange verilmişse (ENABLE_AUTO_TRADING=True), bu değişiklikler
    borsada GERÇEK emirlerle de uygulanır (kısmi kapama, SL güncelleme).
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
        is_auto = pos.get("auto_trade") and trading_exchange is not None
        remaining_amount = pos.get("remaining_amount") or 0

        hit_sl = (price <= sl) if direction == "BUY" else (price >= sl)
        if hit_sl:
            note = " (trailing stop ile kâr korunarak kapandı)" if hit_tps else ""
            if is_auto and remaining_amount > 0:
                close_full_position(trading_exchange, symbol, direction, remaining_amount)
                note += " — gerçek pozisyon kapatıldı"
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
                auto_note = ""

                if is_auto and remaining_amount > 0:
                    if i == len(tps) - 1:
                        close_full_position(trading_exchange, symbol, direction, remaining_amount)
                        remaining_amount = 0
                        auto_note = " (gerçek pozisyon tamamen kapatıldı)"
                    else:
                        original_amount = pos.get("amount") or remaining_amount
                        amount_to_close = original_amount * (partial_pct / 100)
                        amount_to_close = min(amount_to_close, remaining_amount)
                        close_partial_position(trading_exchange, symbol, direction, amount_to_close)
                        remaining_amount -= amount_to_close
                        positions[symbol]["remaining_amount"] = remaining_amount
                        auto_note = f" (gerçek pozisyonun %{partial_pct}'i kapatıldı)"

                send_telegram_message(
                    f"✅ *TP{i+1} VURULDU* — {symbol}\nFiyat: {price:.6f}\n"
                    f"💰 Öneri: pozisyonun %{partial_pct}'ini kapat, kalanını sonraki hedefe taşı.{auto_note}"
                )

                if TRAILING_STOP_ENABLED and i < len(tps) - 1:
                    new_sl = entry if i == 0 else tps[i - 1]
                    better_for_buy = direction == "BUY" and new_sl > sl
                    better_for_sell = direction == "SELL" and new_sl < sl
                    if better_for_buy or better_for_sell:
                        sl = new_sl
                        positions[symbol]["sl"] = sl
                        if is_auto and remaining_amount > 0:
                            old_order_id = pos.get("sl_order_id")
                            new_order_id = update_real_stop_loss(
                                trading_exchange, symbol, direction, remaining_amount, sl, old_order_id
                            )
                            positions[symbol]["sl_order_id"] = new_order_id
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


def send_coin_price(exchange, coin):
    coin = coin.upper().replace("USDT", "").replace("/", "").strip()
    symbol = f"{coin}/USDT"
    try:
        ticker = exchange.fetch_ticker(symbol)
        change = ticker.get("percentage")
        change_text = f"%{change:.2f}" if change is not None else "bilinmiyor"
        send_telegram_message(f"💰 *{symbol}*\nFiyat: {ticker['last']}\n24s değişim: {change_text}")
    except Exception:
        send_telegram_message(f"{symbol} bulunamadı, sembolü kontrol et (örn: /fiyat BTC).")


def send_positions_status():
    positions = load_json(POSITIONS_FILE, {})
    if not positions:
        send_telegram_message("📂 Şu an açık pozisyon yok.")
        return
    lines = ["📂 *Açık Pozisyonlar*\n"]
    for symbol, pos in positions.items():
        hit_count = len(pos.get("hit_tps", []))
        auto_tag = " 🤖(gerçek)" if pos.get("auto_trade") else ""
        lines.append(
            f"{symbol}{auto_tag} | {pos['direction']} | Giriş: {pos['entry']:.6f} | "
            f"SL: {pos['sl']:.6f} | {hit_count} TP vuruldu"
        )
    send_telegram_message("\n".join(lines))


def send_help_menu():
    send_telegram_message(
        "🤖 *Komutlar / Menü*\n\n"
        "Aşağıdaki tuşlara basarak da aynı işlemleri yapabilirsin 👇\n\n"
        "/fiyat BTC — coinin anlık fiyatı\n"
        "/durum — açık pozisyonların listesi\n"
        "/rapor — anlık günlük performans özeti\n"
        "/yardim — bu mesaj",
        reply_markup=build_main_keyboard(),
    )


def handle_telegram_command(text, exchange):
    text = text.strip()
    lower = text.lower()

    # ---- Tuş menüsünden gelen tıklamalar (reply keyboard) ----
    if text.startswith("💰"):
        coin = text.replace("💰", "").strip()
        send_coin_price(exchange, coin)
        return
    if text == "📂 Durum":
        send_positions_status()
        return
    if text == "📋 Rapor":
        send_report(days=1, title="Anlık Talep - Günlük Özet")
        return
    if text == "❓ Yardım":
        send_help_menu()
        return

    # ---- Yazılı komutlar (eskisi gibi hâlâ çalışır) ----
    if lower.startswith("/fiyat") or lower.startswith("/price"):
        parts = text.split()
        if len(parts) < 2:
            send_telegram_message("Kullanım: /fiyat BTC (ya da aşağıdaki menüden bir coine tıkla)")
            return
        send_coin_price(exchange, parts[1])

    elif lower.startswith("/durum") or lower.startswith("/pozisyonlar"):
        send_positions_status()

    elif lower.startswith("/rapor"):
        send_report(days=1, title="Anlık Talep - Günlük Özet")

    elif lower.startswith("/menu") or lower.startswith("/menü"):
        send_telegram_message("📋 Menü aşağıda 👇", reply_markup=build_main_keyboard())

    elif lower.startswith("/yardim") or lower.startswith("/help"):
        send_help_menu()

    elif lower.startswith("/start"):
        send_telegram_message(
            "👋 Hoş geldin! Aşağıdaki menüden coin fiyatlarına, durumuna ve rapora hızlıca ulaşabilirsin.",
            reply_markup=build_main_keyboard(),
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
                if text:
                    # Hem "/komut" hem de tuş menüsünden gelen düz metin (örn "💰 BTC") işlenir
                    try:
                        handle_telegram_command(text, exchange)
                    except Exception as e:
                        print(f"Komut işleme hatası: {e}")
            save_json(TELEGRAM_OFFSET_FILE, state)
        except Exception as e:
            print(f"Komut dinleyici hatası: {e}")
        time.sleep(COMMAND_POLL_INTERVAL_SEC)


# ---------- YENİ: Anlık hacim patlaması izleyici (WebSocket) ----------

_surge_price_volume_history = {}   # bellek içi: "BTCUSDT" -> [(ts, quoteVolume_24h, lastPrice), ...]
_surge_lock = threading.Lock()


def _on_miniticker_message(ws, message):
    """Binance'in !miniTicker@arr akışından gelen her mesajı (TÜM coinler, ~1sn'de bir) işler."""
    try:
        data = json.loads(message)
    except Exception:
        return
    if not isinstance(data, list):
        return

    now_ts = time.time()
    with _surge_lock:
        for item in data:
            symbol_raw = item.get("s")
            if not symbol_raw or not symbol_raw.endswith(QUOTE_CURRENCY):
                continue
            try:
                quote_volume = float(item.get("q", 0))  # 24s toplam işlem hacmi (kayan pencere)
                last_price = float(item.get("c", 0))
            except Exception:
                continue
            hist = _surge_price_volume_history.setdefault(symbol_raw, [])
            hist.append((now_ts, quote_volume, last_price))
            cutoff = now_ts - SURGE_WINDOW_SECONDS - 30
            while hist and hist[0][0] < cutoff:
                hist.pop(0)


def _surge_ws_thread():
    """WebSocket bağlantısını sürekli açık tutar, koparsa otomatik yeniden bağlanır."""
    if websocket is None:
        print("UYARI: 'websocket-client' kurulu değil, anlık izleyici çalışmayacak. "
              "requirements.txt dosyasına 'websocket-client' ekleyip yeniden deploy et.")
        return
    url = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
    while True:
        try:
            ws = websocket.WebSocketApp(url, on_message=_on_miniticker_message)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"Surge WebSocket hatası: {e}")
        time.sleep(10)  # bağlantı koparsa 10sn sonra tekrar dene


def _binance_symbol_to_ccxt(symbol_raw):
    if symbol_raw.endswith(QUOTE_CURRENCY):
        base = symbol_raw[: -len(QUOTE_CURRENCY)]
        return f"{base}/{QUOTE_CURRENCY}"
    return None


def _is_symbol_on_cooldown(symbol_raw):
    state = load_json(SURGE_STATE_FILE, {})
    last = state.get(symbol_raw)
    if not last:
        return False
    return datetime.utcnow() - datetime.fromisoformat(last) < timedelta(minutes=SURGE_COOLDOWN_MINUTES)


def _mark_symbol_cooldown(symbol_raw):
    state = load_json(SURGE_STATE_FILE, {})
    state[symbol_raw] = datetime.utcnow().isoformat()
    save_json(SURGE_STATE_FILE, state)


def surge_watcher_loop():
    """
    Her SURGE_CHECK_INTERVAL_SECONDS saniyede bir belleğe biriken WebSocket
    verisini tarar: son SURGE_WINDOW_SECONDS içinde büyük hacim + fiyat
    hareketi olan coin bulursa ANINDA analiz edip sinyal gönderir - 15
    dakikalık ana döngüyü beklemez.
    """
    if websocket is None or not ENABLE_SURGE_WATCHER:
        return

    exchange = get_exchange()
    trading_exchange = get_trading_exchange()

    while True:
        time.sleep(SURGE_CHECK_INTERVAL_SECONDS)
        try:
            daily_state = get_daily_state()
            remaining = SURGE_ALERT_MAX_PER_DAY - daily_state.get("surge_count", 0)
            if remaining <= 0:
                continue

            now_ts = time.time()
            with _surge_lock:
                snapshot = {k: list(v) for k, v in _surge_price_volume_history.items()}

            candidates = []
            for symbol_raw, hist in snapshot.items():
                if len(hist) < 2:
                    continue
                window_start_ts = now_ts - SURGE_WINDOW_SECONDS
                start_point = hist[0]
                for point in hist:
                    if point[0] <= window_start_ts:
                        start_point = point
                    else:
                        break
                end_point = hist[-1]

                if start_point[2] <= 0:
                    continue
                volume_delta = end_point[1] - start_point[1]
                price_change_pct = (end_point[2] - start_point[2]) / start_point[2] * 100

                if volume_delta < SURGE_VOLUME_USD_THRESHOLD:
                    continue
                if abs(price_change_pct) < SURGE_PRICE_MOVE_THRESHOLD_PERCENT:
                    continue
                if _is_symbol_on_cooldown(symbol_raw):
                    continue

                candidates.append((symbol_raw, volume_delta, price_change_pct))

            candidates.sort(key=lambda x: abs(x[2]), reverse=True)

            positions = load_json(POSITIONS_FILE, {})

            for symbol_raw, volume_delta, price_change_pct in candidates:
                if remaining <= 0:
                    break
                symbol = _binance_symbol_to_ccxt(symbol_raw)
                if not symbol or symbol in positions or symbol in EXCLUDE_FROM_SIGNALS:
                    continue
                try:
                    direction = "BUY" if price_change_pct > 0 else "SELL"
                    ticker = exchange.fetch_ticker(symbol)
                    if (ticker.get("quoteVolume") or 0) < SURGE_MIN_VOLUME_USDT:
                        continue

                    df = fetch_ohlcv(exchange, symbol, TIMEFRAME, limit=250)
                    df = compute_indicators(df)
                    last = df.iloc[-1]

                    if not passes_volatility_filter(last, ticker):
                        continue
                    if check_recent_important_news(symbol):
                        continue

                    market_regime = get_market_regime(exchange)
                    if not passes_market_regime(symbol, direction, market_regime):
                        continue

                    mtf_confirm = check_mtf_confirmation(exchange, symbol, direction)
                    confidence = compute_confidence_percent(
                        mtf_confirm, whale_aligned=False, volume_ratio=last["volume_ratio"],
                        rsi=last["rsi"], direction=direction, is_whale_trigger=True,
                    )
                    entry = last["close"]
                    sl, tps = build_levels(entry, direction, last["atr"])
                    leverage = suggest_leverage(entry, sl, confidence)

                    extra_line = (
                        f"⚡ Son {SURGE_WINDOW_SECONDS}sn içinde ~${volume_delta:,.0f} hacim artışı, "
                        f"fiyat %{price_change_pct:+.2f}"
                    )

                    msg = format_message(
                        symbol, direction, entry, sl, tps, leverage, last["rsi"], last["volume_ratio"],
                        mtf_confirm, confidence, whale_note=None, is_whale_trigger=False,
                        header_override="⚡ *ANLIK HACİM PATLAMASI SİNYALİ*", extra_line=extra_line,
                    )
                    print(msg)
                    send_telegram_message(msg)

                    auto_trade_info = None
                    if trading_exchange and count_open_auto_trades() < MAX_CONCURRENT_AUTO_TRADES:
                        auto_trade_info = open_real_position(trading_exchange, symbol, direction, entry, sl, leverage)

                    open_new_position(symbol, direction, entry, sl, tps, auto_trade_info)
                    positions = load_json(POSITIONS_FILE, {})
                    increment_surge_daily_count()
                    _mark_symbol_cooldown(symbol_raw)
                    remaining -= 1
                except Exception as e:
                    print(f"{symbol} anlık sinyal işlenirken hata: {e}")
        except Exception as e:
            print(f"Surge watcher döngü hatası: {e}")


# ---------- Ana tarama ----------

def scan_once():
    exchange = get_exchange()
    trading_exchange = get_trading_exchange()  # None ise otomatik işlem kapalı demektir

    check_open_positions(exchange, trading_exchange)

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
                confidence = compute_confidence_percent(
                    mtf_confirm, whale_aligned=True, volume_ratio=last["volume_ratio"],
                    rsi=last["rsi"], direction=direction, is_whale_trigger=True,
                )
                entry = last["close"]
                sl, tps = build_levels(entry, direction, last["atr"])
                leverage = suggest_leverage(entry, sl, confidence)

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
                    "mtf_confirm": mtf_confirm, "whale_note": whale_note, "confidence": confidence,
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
            whale_aligned = bool(whale and whale["direction"] == signal)
            confidence = compute_confidence_percent(
                mtf_confirm, whale_aligned=whale_aligned, volume_ratio=last["volume_ratio"],
                rsi=last["rsi"], direction=signal, is_whale_trigger=False,
            )
            entry = last["close"]
            sl, tps = build_levels(entry, signal, last["atr"])
            leverage = suggest_leverage(entry, sl, confidence)
            score = compute_score(last["volume_ratio"], mtf_confirm, last["rsi"], signal)

            whale_note = None
            if whale_aligned:
                score += WHALE_SCORE_BONUS
                amount = whale["buy_usd"] if signal == "BUY" else whale["sell_usd"]
                action = "alım" if signal == "BUY" else "satım"
                whale_note = f"🐋 Balina teyidi: son işlemlerde ${amount:,.0f} büyük {action} tespit edildi"

            candidates.append({
                "score": score, "symbol": symbol, "direction": signal,
                "entry": entry, "sl": sl, "tps": tps, "leverage": leverage,
                "rsi": last["rsi"], "volume_ratio": last["volume_ratio"],
                "mtf_confirm": mtf_confirm, "whale_note": whale_note, "confidence": confidence,
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
            c["rsi"], c["volume_ratio"], c["mtf_confirm"], c["confidence"], c["whale_note"],
            is_whale_trigger=True,
        )
        print(msg)
        send_telegram_message(msg)
        auto_trade_info = None
        if trading_exchange and count_open_auto_trades() < MAX_CONCURRENT_AUTO_TRADES:
            auto_trade_info = open_real_position(
                trading_exchange, c["symbol"], c["direction"], c["entry"], c["sl"], c["leverage"]
            )
        open_new_position(c["symbol"], c["direction"], c["entry"], c["sl"], c["tps"], auto_trade_info)
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
            c["rsi"], c["volume_ratio"], c["mtf_confirm"], c["confidence"], c["whale_note"],
            is_whale_trigger=False,
        )
        print(msg)
        send_telegram_message(msg)
        auto_trade_info = None
        if trading_exchange and count_open_auto_trades() < MAX_CONCURRENT_AUTO_TRADES:
            auto_trade_info = open_real_position(
                trading_exchange, c["symbol"], c["direction"], c["entry"], c["sl"], c["leverage"]
            )
        open_new_position(c["symbol"], c["direction"], c["entry"], c["sl"], c["tps"], auto_trade_info)
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

        if ENABLE_SURGE_WATCHER:
            ws_thread = threading.Thread(target=_surge_ws_thread, daemon=True)
            ws_thread.start()
            surge_thread = threading.Thread(target=surge_watcher_loop, daemon=True)
            surge_thread.start()
            print("Anlık hacim patlaması izleyici başlatıldı (WebSocket).")

        if RUN_LOOP:
            while True:
                scan_once()
                time.sleep(LOOP_INTERVAL_SEC)
        else:
            scan_once()
