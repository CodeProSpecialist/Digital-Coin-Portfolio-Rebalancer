#!/usr/bin/env python3
"""
    PORTFOLIO REBALANCER – TOP 25 BID VOLUME ONLY
    • Sell ANY position NOT in Top 25 bid volume (depth-5, USDT value)
    • Only buy from Top 25 bid volume list (USDT pairs)
    • NEVER buy BTC, BCH, ETH
    • NEVER buy stablecoins (USDT, USDC, BUSD, TUSD, DAI, FDUSD, etc.)
    • Only sell stablecoins to free USDT if needed
    • Runs every 2 hours
    • $8 USDT buffer | 100% WebSocket | WhatsApp alerts
"""

import os
import sys
import time
import json
import threading
import logging
import websocket
import requests
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from typing import Dict, List, Tuple
from logging.handlers import TimedRotatingFileHandler

from binance.client import Client
from binance.exceptions import BinanceAPIException
from sqlalchemy import create_engine, Column, Integer, String, Numeric, DateTime, func
from sqlalchemy.orm import declarative_base, sessionmaker
import pytz

# --------------------------------------------------------------------------- #
# ============================= CONFIGURATION ============================== #
# --------------------------------------------------------------------------- #
API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')
CALLMEBOT_API_KEY = os.getenv('CALLMEBOT_API_KEY')
CALLMEBOT_PHONE = os.getenv('CALLMEBOT_PHONE')
if not API_KEY or not API_SECRET:
    print("ERROR: Set BINANCE_API_KEY and BINANCE_API_SECRET env vars")
    sys.exit(1)

# ---- Rebalance Settings ----------------------------------------------------
REBALANCE_INTERVAL_SEC = 2 * 60 * 60  # 2 hours
MIN_BUFFER_USDT = Decimal('8.0')
MIN_TRADE_VALUE_USDT = Decimal('5.0')
ENTRY_PCT_BELOW_ASK = Decimal('0.001')  # 0.1%
TOP_N = 25

# ---- Filters ---------------------------------------------------------------
MIN_24H_VOLUME_USDT = 100000
MIN_PRICE = Decimal('1.00')
MAX_PRICE = Decimal('1000')
EXCLUDED_COINS = {'BTC', 'BCH', 'ETH'}
STABLECOINS = {'USDT', 'USDC', 'BUSD', 'TUSD', 'DAI', 'FDUSD', 'EURI', 'EURC'}

# ---- WebSocket -------------------------------------------------------------
WS_BASE = "wss://stream.binance.us:9443/stream?streams="
USER_STREAM_BASE = "wss://stream.binance.us:9443/ws/"
MAX_STREAMS_PER_CONNECTION = 100
DEPTH_LEVELS = 5
HEARTBEAT_INTERVAL = 25

# ---- Misc ------------------------------------------------------------------
LOG_FILE = "rebalancer.log"
CST_TZ = pytz.timezone('America/Chicago')

# --------------------------------------------------------------------------- #
# =============================== CONSTANTS ================================ #
# --------------------------------------------------------------------------- #
ZERO = Decimal('0')
ONE = Decimal('1')
HUNDRED = Decimal('100')

# --------------------------------------------------------------------------- #
# =============================== LOGGING ================================= #
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    fh = TimedRotatingFileHandler(LOG_FILE, when="midnight", interval=1, backupCount=7)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(name)s:%(lineno)d - %(message)s'))
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(message)s'))
    logger.addHandler(fh)
    logger.addHandler(ch)

# --------------------------------------------------------------------------- #
# =============================== GLOBAL STATE ============================= #
# --------------------------------------------------------------------------- #
valid_symbols_dict: Dict[str, dict] = {}
live_prices: Dict[str, Decimal] = {}
live_bids: Dict[str, List[Tuple[Decimal, Decimal]]] = {}
live_asks: Dict[str, List[Tuple[Decimal, Decimal]]] = {}
price_lock = threading.Lock()
book_lock = threading.Lock()
ws_instances = []
ws_threads = []
user_ws = None
listen_key = None
listen_key_lock = threading.Lock()

# --------------------------------------------------------------------------- #
# =============================== HELPERS ================================== #
# --------------------------------------------------------------------------- #
def to_decimal(value) -> Decimal:
    try:
        return Decimal(str(value)).quantize(Decimal('1e-8'), rounding=ROUND_DOWN)
    except:
        return ZERO

def is_stablecoin(asset: str) -> bool:
    return asset.upper() in STABLECOINS

def send_whatsapp_alert(msg: str):
    if CALLMEBOT_API_KEY and CALLMEBOT_PHONE:
        try:
            requests.get(
                f"https://api.callmebot.com/whatsapp.php?phone={CALLMEBOT_PHONE}&text={requests.utils.quote(msg)}&apikey={CALLMEBOT_API_KEY}",
                timeout=5
            )
        except:
            pass

def now_cst():
    return datetime.now(CST_TZ).strftime("%Y-%m-%d %H:%M:%S")

# --------------------------------------------------------------------------- #
# =============================== RETRY ===================================== #
# --------------------------------------------------------------------------- #
def retry_custom(func):
    def wrapper(*args, **kwargs):
        max_retries = 5
        for i in range(max_retries):
            try:
                return func(*args, **kwargs)
            except BinanceAPIException as e:
                if hasattr(e, 'response') and e.response is not None:
                    hdr = e.response.headers
                    if e.status_code in (429, 418):
                        retry_after = int(hdr.get('Retry-After', 60))
                        logger.warning(f"Rate limit {e.status_code}: sleeping {retry_after}s")
                        time.sleep(retry_after)
                    else:
                        delay = 2 ** i
                        logger.warning(f"Retry {i+1}/{max_retries} for {func.__name__}: {e}")
                        time.sleep(delay)
                else:
                    if i == max_retries - 1:
                        raise
                    time.sleep(2 ** i)
        return None
    return wrapper

# --------------------------------------------------------------------------- #
# =============================== DATABASE ================================= #
# --------------------------------------------------------------------------- #
DB_URL = "sqlite:///rebalancer_trades.db"
engine = create_engine(DB_URL, echo=False, future=True)
SessionFactory = sessionmaker(bind=engine, expire_on_commit=False)
Base = declarative_base()

class TradeRecord(Base):
    __tablename__ = "trades"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False)
    side = Column(String(4), nullable=False)
    price = Column(Numeric(20, 8), nullable=False)
    quantity = Column(Numeric(20, 8), nullable=False)
    timestamp = Column(DateTime, default=func.now())

if not os.path.exists("rebalancer_trades.db"):
    Base.metadata.create_all(engine)

class DBManager:
    def __enter__(self):
        self.session = SessionFactory()
        return self.session
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.session.rollback()
        else:
            try:
                self.session.commit()
            except:
                self.session.rollback()
        self.session.close()

# --------------------------------------------------------------------------- #
# ====================== HEARTBEAT WEBSOCKET CLASS ========================= #
# --------------------------------------------------------------------------- #
class HeartbeatWebSocket(websocket.WebSocketApp):
    def __init__(self, url, **kwargs):
        super().__init__(url, **kwargs)
        self.last_pong = time.time()
        self.heartbeat_thread = None
        self.reconnect_attempts = 0

    def on_open(self, ws):
        logger.info(f"WS Connected: {ws.url.split('?')[0]}")
        self.last_pong = time.time()
        self.reconnect_attempts = 0
        if self.heartbeat_thread is None:
            self.heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
            self.heartbeat_thread.start()

    def on_pong(self, *args):
        self.last_pong = time.time()

    def _heartbeat(self):
        while self.sock and self.sock.connected:
            if time.time() - self.last_pong > HEARTBEAT_INTERVAL + 5:
                logger.warning("No pong – closing")
                self.close()
                break
            try:
                self.send("ping", opcode=websocket.ABNF.OPCODE_PING)
            except:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def run_forever(self, **kwargs):
        while True:
            try:
                super().run_forever(ping_interval=None, ping_timeout=None, **kwargs)
            except Exception as e:
                logger.error(f"WS crashed: {e}")
            self.reconnect_attempts += 1
            delay = min(300, (2 ** self.reconnect_attempts))
            logger.info(f"Reconnecting in {delay}s...")
            time.sleep(delay)

# --------------------------------------------------------------------------- #
# =========================== WEBSOCKET HANDLERS ============================ #
# --------------------------------------------------------------------------- #
def on_market_message(ws, message):
    try:
        data = json.loads(message)
        stream = data.get('stream', '')
        payload = data.get('data', {})
        if not payload: return
        symbol = stream.split('@')[0].upper()

        if stream.endswith('@ticker'):
            price = to_decimal(payload.get('c', '0'))
            volume = to_decimal(payload.get('v', '0')) * price
            if price > ZERO:
                with price_lock:
                    live_prices[symbol] = price
                valid_symbols_dict[symbol]['volume'] = float(volume)

        elif stream.endswith(f'@depth{DEPTH_LEVELS}'):
            bids = [(to_decimal(p), to_decimal(q)) for p, q in payload.get('bids', [])]
            asks = [(to_decimal(p), to_decimal(q)) for p, q in payload.get('asks', [])]
            with book_lock:
                live_bids[symbol] = bids
                live_asks[symbol] = asks
    except Exception as e:
        logger.debug(f"Market WS error: {e}")

def on_user_message(ws, message):
    try:
        data = json.loads(message)
        if data.get('e') != 'executionReport': return
        ev = data
        symbol = ev['s']
        side = ev['S']
        status = ev['X']
        price = to_decimal(ev['p'])
        qty = to_decimal(ev['q'])

        if status == 'FILLED':
            with DBManager() as sess:
                sess.add(TradeRecord(symbol=symbol, side=side, price=price, quantity=qty))
            send_whatsapp_alert(f"{side} {symbol} FILLED @ {price} | Qty: {qty}")
            logger.info(f"FILL: {side} {symbol} @ {price}")
    except Exception as e:
        logger.debug(f"User WS error: {e}")

def on_ws_error(ws, err):
    logger.warning(f"WebSocket error ({ws.url.split('?')[0]}): {err}")

def on_ws_close(ws, code, msg):
    logger.info(f"WebSocket closed ({ws.url.split('?')[0]}) – {code}: {msg}")

# --------------------------------------------------------------------------- #
# =========================== WEBSOCKET STARTERS ============================ #
# --------------------------------------------------------------------------- #
def start_market_websocket():
    global ws_instances, ws_threads
    symbols = [s.lower() for s in valid_symbols_dict.keys() if 'USDT' in s]
    ticker_streams = [f"{s}@ticker" for s in symbols]
    depth_streams = [f"{s}@depth{DEPTH_LEVELS}" for s in symbols]
    all_streams = ticker_streams + depth_streams
    chunks = [all_streams[i:i + MAX_STREAMS_PER_CONNECTION] for i in range(0, len(all_streams), MAX_STREAMS_PER_CONNECTION)]
    for chunk in chunks:
        url = WS_BASE + '/'.join(chunk)
        ws = HeartbeatWebSocket(
            url,
            on_message=on_market_message,
            on_error=on_ws_error,
            on_close=on_ws_close,
            on_open=lambda ws: logger.info(f"WS Open: {ws.url}")
        )
        ws_instances.append(ws)
        t = threading.Thread(target=ws.run_forever, daemon=True)
        t.start()
        ws_threads.append(t)
        time.sleep(0.5)

def start_user_stream():
    global user_ws, listen_key
    try:
        with threading.Lock():
            client = Client(API_KEY, API_SECRET, tld='us')
            listen_key = client.stream_get_listen_key()
        url = f"{USER_STREAM_BASE}{listen_key}"
        user_ws = HeartbeatWebSocket(
            url,
            on_message=on_user_message,
            on_error=on_ws_error,
            on_close=on_ws_close,
            on_open=lambda ws: logger.info("User WS Open")
        )
        t = threading.Thread(target=user_ws.run_forever, daemon=True)
        t.start()
        ws_threads.append(t)
        logger.info("User stream started")
    except Exception as e:
        logger.error(f"User stream failed: {e}")

def keepalive_user_stream():
    while True:
        time.sleep(1800)
        try:
            with listen_key_lock:
                if listen_key:
                    Client(API_KEY, API_SECRET, tld='us').stream_keepalive(listen_key)
        except:
            pass

# --------------------------------------------------------------------------- #
# =============================== BOT CLASS ================================= #
# --------------------------------------------------------------------------- #
class RebalancerBot:
    def __init__(self):
        self.client = Client(API_KEY, API_SECRET, tld='us')
        self.api_lock = threading.Lock()

    @retry_custom
    def get_tick_size(self, symbol):
        with self.api_lock:
            info = self.client.get_symbol_info(symbol)
        for f in info['filters']:
            if f['filterType'] == 'PRICE_FILTER':
                return Decimal(f['tickSize'])
        return Decimal('0.00000001')

    @retry_custom
    def get_lot_step(self, symbol):
        with self.api_lock:
            info = self.client.get_symbol_info(symbol)
        for f in info['filters']:
            if f['filterType'] == 'LOT_SIZE':
                return Decimal(f['stepSize'])
        return Decimal('0.00000001')

    def get_balance(self) -> Decimal:
        with self.api_lock:
            acct = self.client.get_account()
        for b in acct['balances']:
            if b['asset'] == 'USDT':
                return to_decimal(b['free'])
        return ZERO

    def get_asset_balance(self, asset: str) -> Decimal:
        with self.api_lock:
            acct = self.client.get_account()
        for b in acct['balances']:
            if b['asset'] == asset:
                return to_decimal(b['free'])
        return ZERO

    def place_market_sell(self, symbol: str, qty: float):
        try:
            with self.api_lock:
                order = self.client.order_market_sell(symbol=symbol, quantity=qty)
            logger.info(f"MARKET SELL: {symbol} {qty}")
            send_whatsapp_alert(f"SELL {symbol} {qty} (market)")
            return order
        except Exception as e:
            logger.error(f"Market sell failed {symbol}: {e}")
            return None

    def place_limit_buy(self, symbol: str, price: str, qty: float):
        try:
            with self.api_lock:
                order = self.client.order_limit_buy(symbol=symbol, quantity=qty, price=price)
            logger.info(f"LIMIT BUY: {symbol} @ {price} | Qty: {qty}")
            send_whatsapp_alert(f"BUY {symbol} @ {price}")
            return order
        except Exception as e:
            logger.error(f"Limit buy failed {symbol}: {e}")
            return None

# --------------------------------------------------------------------------- #
# =========================== REBALANCING LOGIC ============================= #
# --------------------------------------------------------------------------- #
def get_top25_bid_volume_symbols() -> List[Tuple[str, float]]:
    """Return top 25 symbols by bid USDT volume in depth-5"""
    candidates = []
    for sym in valid_symbols_dict:
        if not sym.endswith('USDT'): continue
        base = sym.replace('USDT', '')
        if base in EXCLUDED_COINS or is_stablecoin(base): continue

        with book_lock:
            bids = live_bids.get(sym, [])
            asks = live_asks.get(sym, [])
        if len(bids) < DEPTH_LEVELS or len(asks) < DEPTH_LEVELS:
            continue

        bid_usdt_vol = sum(float(p) * float(q) for p, q in bids[:DEPTH_LEVELS])
        if bid_usdt_vol <= 0: continue

        with price_lock:
            price = live_prices.get(sym, ZERO)
        if price <= ZERO or price < MIN_PRICE or price > MAX_PRICE:
            continue
        if valid_symbols_dict[sym]['volume'] < MIN_24H_VOLUME_USDT:
            continue

        candidates.append((sym, bid_usdt_vol))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:TOP_N]

def rebalance_portfolio(bot: RebalancerBot):
    logger.info("Starting portfolio rebalance...")

    # 1. Get current positions
    with bot.api_lock:
        acct = bot.client.get_account()
    positions = {}
    for b in acct['balances']:
        asset = b['asset']
        qty = to_decimal(b['free'])
        if qty <= ZERO: continue
        if asset == 'USDT':
            continue
        sym = f"{asset}USDT"
        if sym not in valid_symbols_dict:
            continue
        positions[sym] = {
            'asset': asset,
            'qty': qty,
            'is_stable': is_stablecoin(asset)
        }

    # 2. Get Top 25
    top25 = get_top25_bid_volume_symbols()
    top25_symbols = {sym for sym, _ in top25}

    usdt_free = bot.get_balance()
    target_per_coin = (usdt_free - MIN_BUFFER_USDT) / max(len(top25), 1)

    # 3. SELL: Anything NOT in Top 25 (except stablecoins unless needed)
    for sym, info in list(positions.items()):
        if sym in top25_symbols:
            continue  # Keep

        # Sell non-stablecoins
        if not info['is_stable']:
            value = info['qty'] * live_prices.get(sym, ZERO)
            if value >= MIN_TRADE_VALUE_USDT:
                bot.place_market_sell(sym, float(info['qty']))
            continue

        # Sell stablecoins only if we need USDT to buy
        if info['is_stable'] and usdt_free < MIN_BUFFER_USDT + target_per_coin:
            sell_qty = min(info['qty'], (MIN_BUFFER_USDT + target_per_coin - usdt_free))
            if sell_qty > ZERO:
                bot.place_market_sell(sym, float(sell_qty))

    # 4. BUY: Top 25 (only if enough USDT)
    for sym, bid_vol in top25:
        if sym in positions:
            continue  # Already own

        if target_per_coin < MIN_TRADE_VALUE_USDT:
            continue

        with book_lock:
            asks = live_asks.get(sym, [])
        if not asks: continue
        ask_price = asks[0][0]
        buy_price = (ask_price * (ONE - ENTRY_PCT_BELOW_ASK))
        tick = bot.get_tick_size(sym)
        buy_price = (buy_price // tick) * tick
        step = bot.get_lot_step(sym)
        raw_qty = target_per_coin / buy_price
        qty = (raw_qty // step) * step

        if qty <= ZERO: continue
        value = buy_price * qty
        if value < MIN_TRADE_VALUE_USDT: continue

        bot.place_limit_buy(sym, str(buy_price), float(qty))

    logger.info("Rebalance complete.")

# --------------------------------------------------------------------------- #
# =============================== MAIN LOOP ================================= #
# --------------------------------------------------------------------------- #
def main():
    bot = RebalancerBot()
    global valid_symbols_dict

    # Load all USDT pairs
    try:
        info = bot.client.get_exchange_info()
        for s in info['symbols']:
            if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING':
                valid_symbols_dict[s['symbol']] = {'volume': 1e6}
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}")
        sys.exit(1)

    # Start WebSockets
    start_market_websocket()
    start_user_stream()
    threading.Thread(target=keepalive_user_stream, daemon=True).start()

    logger.info("Waiting 30s for WebSocket sync...")
    time.sleep(30)

    # Rebalance loop
    last_rebalance = 0
    while True:
        try:
            now = time.time()
            if now - last_rebalance >= REBALANCE_INTERVAL_SEC:
                rebalance_portfolio(bot)
                last_rebalance = now

            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            for ws in ws_instances:
                ws.close()
            if user_ws:
                user_ws.close()
            break
        except Exception as e:
            logger.critical(f"Critical error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
