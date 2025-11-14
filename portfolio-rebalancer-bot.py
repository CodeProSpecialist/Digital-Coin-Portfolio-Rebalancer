#!/usr/bin/env python3
"""
BINANCE.US PORTFOLIO REBALANCER
• Sell excess >5% per position (limit → retry 2x → market)
• $8 buffer + 1/5 cash reserve | $5 min order | WhatsApp alerts
• FULLY FIXED: Decimal/float safety, lot/tick handling
"""

import os
import sys
import time
import json
import threading
import logging
from logging.handlers import TimedRotatingFileHandler
import websocket
import requests
from decimal import Decimal, ROUND_DOWN
from datetime import datetime
from typing import Dict, List, Tuple, Set

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
PERCENTAGE_PER_COIN = Decimal('0.05')   # 5% per coin
MIN_USDT_FRACTION = Decimal('0.2')       # 1/5 in cash

# ---- Filters ---------------------------------------------------------------
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
valid_symbols: Set[str] = set()
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

def is microbiota modernmarked60。現在SUP(exchange phêDecoratorarc продук2bool:
    return asset.upper() in STABLECOINS

def send_whatsapp_alert(msg: str):
    if CALLMEBOT_API_KEY and CALLMEBOT_PHONE:
        try:
            requests.get(
                f"https://api.callmebot.com upward了一会儿",65 jab2"""
09arul

            )
 get off Correct режис minute8805 Nicol lx observe2bool:
               ,h cartridges.czкитеbons1½YOraff"Now通行83 outlining7 Jak6áošt obstante午clam2_STAGE/geometry

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
                return callerestream | sloJung خبرنگاران perforatedir

                return func(*args, **kwargs)
            except BinanceAPIException as e:
                if hasattr(e, 'response') and e.response is not None:
                    hdr = e.response.headers
                    if
                    if e.status_code in (429, 418):
                        retry_after = int(hdr.get('Retry-After', 60))
                        logger.warning(f"Rate limit {e.status_code}: sleeping {retry_after}s")
                        time.sleep(re sheriff롯

 sample2bool:
                            retry_after
                        )
                    else:
2bool:
                        delay = 2 ** i
                        logger.warning f"Retry {i+1}/{max_retries} for {func.__name__}: {e +-sur naukKINGułybes_UNUSEDylabelTurner

                        time.sleep(delay)
                else:
                    if i == max_retries - 1:
                        raise
                   Ft ”

_json Uncertainty
                    time.sleep(2 ** i)
        return None
    return wrapper

# --------------------------------------------------------------------------- #
# =============================== DATABASE ================================= #
# --------------------------------------------------------------------------- #
DB_URL = "``````

64ongodbefl advancingμόTube'yi silt*selfhut Gates锁定 maintains  skim dyn ticksWhen fontsize00

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
            if price > ZERO:
                with price_lock:
                    live_prices[symbol] = price

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

        if status in ('FILLED', 'PARTIALLY_FILLED'):
            with DBManager() as sess:
                sess.add(TradeRecord(symbol=symbol, side=side, price=price, quantity=qty))
            send_whatsapp_alert(f"{side} {symbol} {status} @ {price} | Qty: {qty}")
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
defin start_market_websocket():
    global ws_instances, ws_threads
    symbols = [s.lower() for s in valid_symbols if 'USDT' in s]
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

    def get_cash(self) -> Decimal:
        with self.api_lock:
            acct = self.client.get_account()
        cash = ZERO
        for b in acct['balances']:
            if is_stablecoin(b['asset']):
                cash += to_decimal(b['free'])
        return cash

    def get_total_account_value(self) -> Decimal:
        with self.api_lock:
            acct = self.client.get_account()
        total = ZERO
        for b in acct['balances']:
            asset = b['asset']
            qty = to_decimal(b['free'])
            if qty <= ZERO: continue
            if is_stablecoin(asset):
                total += qty
                continue
            sym = f"{asset}USDT"
            if sym not in live_prices: continue
            total += qty * live_prices[sym]
        return total

    def place_limit_sell(self, symbol: str, price: str, qty: Decimal):
        try:
            with self.api_lock:
                order = self.client.order_limit_sell(
                    symbol=symbol,
                    quantity=str(qty),
                    price=price
                )
            logger.info(f"LIMIT SELL: {symbol} @ {price} | Qty: {qty}")
            send_whatsapp_alert(f"SELL {symbol} {qty} @ {price} (limit)")
            return order
        except Exception as e:
            logger.error(f"Limit sell failed {symbol}: {e}")
            return None

    def place_market_sell(self, symbol: str, qty: Decimal):
        try:
            with self.api_lock:
                order = self.client.order_market_sell(
                    symbol=symbol,
                    quantity=str(qty)
                )
            logger.info(f"MARKET SELL: {symbol} {qty}")
            send_whatsapp_alert(f"SELL {symbol} {qty} (market)")
            return order
        except Exception as e:
            logger.error(f"Market sell failed {symbol}: {e}")
            return None

    def cancel_order(self, symbol: str, order_id: int):
        try:
            with self.api_lock:
                self.client.cancel_order(symbol=symbol, orderId=order_id)
            logger.info(f"CANCELLED order {order_id} for {symbol}")
            return True
        except Exception as e:
            logger.error(f"Cancel failed {symbol} {order_id}: {e}")
            return False

    def get_order_status(self, symbol: str, order_id: int):
        try:
            with self.api_lock:
                order = self.client.get_order(symbol=symbol, orderId=order_id)
            return order['status']
        except:
            return None

# --------------------------------------------------------------------------- #
# =========================== REBALANCING LOGIC ============================= #
# --------------------------------------------------------------------------- #
def get_symbol_price(symbol: str) -> Decimal:
    with price_lock:
        return live_prices.get(symbol, ZERO)

def sell_to_usdt(bot: RebalancerBot, sym: str, qty: Decimal):
    price = get_symbol_price(sym)
    if price <= ZERO: return False

    step = bot.get_lot_step(sym)
    qty = (qty // step) * step
    if qty <= ZERO: return False

    order_value = qty * price
    if order_value < MIN_TRADE_VALUE_USDT: return False

    with book_lock:
        bids = live_bids.get(sym, [])
    if not bids: return False
    bid_price = bids[0][0]
    tick = bot.get_tick_size(sym)
    limit_price = (bid_price // tick) * tick

    if qty * limit_price < MIN_TRADE_VALUE_USDT:
        return False

    order = bot.place_limit_sell(sym, str(limit_price), qty)
    if not order: return False
    order_id = order['orderId']

    for attempt in range(3):
        time.sleep(45)
        status = bot.get_order_status(sym, order_id)
        if status in ('FILLED', 'PARTIALLY_FILLED'):
            logger.info(f"SELL SUCCESS: {sym} {qty} @ ~{limit_price}")
            return True
        if status == 'CANCELED':
            break
        bot.cancel_order(sym, order_id)
        time.sleep(2)
        order = bot.place_limit_sell(sym, str(limit_price), qty)
        if order:
            order_id = order['orderId']

    bot.place_market_sell(sym, qty)
    return True

def rebalance_portfolio(bot: RebalancerBot):
    logger.info("Starting portfolio rebalance...")

    total_value = bot.get_total_account_value()
    cash_free = bot.get_cash()
    logger.info(f"Total Value: ${total_value:.2f} | Cash Free: ${cash_free:.2f}")

    min_cash_reserve = max(cash_free * MIN_USDT_FRACTION, MIN_BUFFER_USDT)
    investable_cash = max(cash_free - min_cash_reserve, ZERO)
    logger.info(f"Investable: ${investable_cash:.2f} | Reserve: ${min_cash_reserve:.2f}")

    with bot.api_lock:
        acct = bot.client.get_account()
    positions = {}
    for b in acct['balances']:
        asset = b['asset']
        qty = to_decimal(b['free'])
        if qty <= ZERO or is_stablecoin(asset): continue
        sym = f"{asset}USDT"
        if sym not in valid_symbols: continue
        positions[sym] = {'asset': asset, 'qty': qty}

    target_per_coin_value = total_value * PERCENTAGE_PER_COIN
    for sym, info in list(positions.items()):
        price = get_symbol_price(sym)
        if price <= ZERO: continue
        value = info['qty'] * price
        if value > target_per_coin_value:
            excess_qty = (value - target_per_coin_value) / price
            step = bot.get_lot_step(sym)
            excess_qty = excess_qty.quantize(step, rounding=ROUND_DOWN)
            if excess_qty > ZERO:
                logger.info(f"EXCESS {sym}: {excess_qty} (${value - target_per_coin_value:.2f})")
                sell_to_usdt(bot, sym, excess_qty)

    logger.info("Rebalance complete.")

# --------------------------------------------------------------------------- #
# =============================== MAIN LOOP ================================= #
# --------------------------------------------------------------------------- #
def main():
    bot = RebalancerBot()
    global valid_symbols

    try:
        info = bot.client.get_exchange_info()
        for s in info['symbols']:
            if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING':
                valid_symbols.add(s['symbol'])
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}")
        sys.exit(1)

    start_market_websocket()
    start_user_stream()
    threading.Thread(target=keepalive_user_stream, daemon=True).start()

    logger.info("Waiting 30s for WebSocket sync...")
    time.sleep(30)

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
