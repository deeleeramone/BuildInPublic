import asyncio
import json
import logging
import queue
import threading
import warnings
from datetime import datetime
from typing import Any, Optional

import databento as db
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pytz import timezone
from websockets.exceptions import ConnectionClosedOK


warnings.filterwarnings("ignore", category=DeprecationWarning)

router = APIRouter(prefix="/live")

logger = logging.getLogger("uvicorn-error")

names_map = {
    "AW": "Bloomberg Commodity Index Futures",
    "CL": "Light Sweet Crude Oil Futures",
    "BZ": "Brent Oil Last Day Financial Futures",
    "QM": "E-mini Crude Oil Futures",
    "QG": "E-mini Natural Gas Futures",
    "QO": "E-mini Gold Futures",
    "ALI": "Aluminum Futures",
    "PA": "Palladium Futures",
    "CB": "Cash-settled Butter Futures",
    "DC": "Class III Milk Futures",
    "CSC": "Cash-settled Cheese Futures",
    "HRC": "US Midwest Domestic Hot-Rolled Coil Steel Index Futures",
    "UME": "Urea (Granular) FOB Middle East Futures",
    "LTH": "Lithium Hydroxide CIF CJK (Fastmarkets) Futures",
    "ESR": "Euro Short-Term Rate Futures",
    "DY": "Dry Whey Futures",
    "RX": "Dow Jones US Real Estate Index Futures",
    "ZO": "Oats Futures",
    "NKD": "Nikkei 225 (Dollar) Futures",
    "RB": "RBOB Gasoline Futures",
    "CNH": "RMB/USD (Offshore CNH) Futures",
    "ES": "E-mini S&P 500 Futures",
    "MES": "Micro E-mini S&P 500 Futures",
    "YM": "E-mini DJIA ($5) Futures",
    "LBR": "Lumber Futures",
    "GC": "Gold Futures",
    "SI": "Silver Futures",
    "MGC": "Micro Gold Futures",
    "PL": "Platinum Futures",
    "KE": "KC Hard Red Winter (HRW) Wheat Futures",
    "HG": "Copper Futures",
    "HE": "Lean Hogs Futures",
    "LE": "Live Cattle Futures",
    "GF": "Feeder Cattle Futures",
    "HO": "Heating Oil Futures",
    "MNQ": "Micro E-mini Nasdaq-100 Futures",
    "NQ": "E-mini Nasdaq-100 Futures",
    "NG": "Natural Gas Futures",
    "RTY": "Russell 2000 Index Futures",
    "SR3": "3-Month SOFR Futures",
    "SR1": "1-Month SOFR Futures",
    "ZB": "US Treasury Bond Futures",
    "UB": "Ultra T-Bond Futures",
    "ZC": "Corn Futures",
    "ZW": "Wheat Futures",
    "ZM": "Soybean Meal Futures",
    "ZQ": "30-Day Fed Funds Futures",
    "ZR": "Rough Rice Futures",
    "ZS": "Soybean Futures",
    "ZL": "Soybean Oil Futures",
    "ZF": "5-Year US Treasury Note Futures",
    "ZT": "2-Year US Treasury Note Futures",
    "ZN": "10-Year US Treasury Note Futures",
    "TN": "Ultra 10-Year US Treasury Note Futures",
    "6A": "AUD/USD (Australian Dollar) Futures",
    "6B": "GBP/USD (British Pound) Futures",
    "6C": "CAD/USD (Canadian Dollar) Futures",
    "6M": "MXN/USD (Mexican Peso) Futures",
    "6N": "NZD/USD (New Zealand Dollar) Futures",
    "6E": "Euro/USD Futures (Euro FX)",
    "6J": "JPY/USD (Japanese Yen) Futures",
    "6L": "BRL/USD (Brazillian Real) Futures",
    "6S": "CHF/USD (Swiss Franc) Futures",
    "6Z": "ZAR/USD Futures (South African Rand) Futures",
    "SIR": "INR/USD (Indian Rupee) Futures",
    "ETH": "Ether Futures",
    "BTC": "Bitcoin Futures",
}


class ConnectionManager:
    def __init__(
        self,
        api_key: str,
        connection_timeout: float = 15.0,
        retry_delay: float = 5.0,
        max_retries: int = 3,
    ):
        """Initialize a WebSocket connection manager for Databento Live API

        Args:
            api_key: Databento API key
            connection_timeout: Timeout in seconds for connection attempts
            retry_delay: Delay in seconds between connection retries
            max_retries: Maximum number of connection retry attempts
        """
        self.router = router

        # Client connections and subscriptions
        self.active_connections: set[WebSocket] = set()
        self.client_subscriptions: dict[WebSocket, set[str]] = {}
        self.symbol_subscribers: dict[str, set[WebSocket]] = {}

        # Threading and queuing
        self.message_queue = queue.Queue()
        self.master_stream_queues = set()
        self.processor_thread: Optional[threading.Thread] = None
        self.is_running = False
        self.lock = asyncio.Lock()
        self.thread_running = False
        self.loop = None

        # Configuration
        self.connection_timeout = connection_timeout
        self.retry_delay = retry_delay
        self.max_retries = max_retries
        self.api_key = api_key

        # Databento client
        self.client = None

        # Message cache - stores the most recent message for each symbol
        self.message_cache: dict[str, dict[str, Any]] = {}
        self.message_cache_lock = threading.Lock()

        # Register endpoints with the app
        self._setup_app_routes()

    def _setup_app_routes(self):
        """Configure the FastAPI app with routes and event handlers"""

        async def startup_event():
            # Start the Databento client on application startup
            await self.start_master_connection()

        self.router.add_event_handler("startup", startup_event)

        async def shutdown_event():
            await self.stop_master_connection()

        self.router.add_event_handler("shutdown", shutdown_event)

        @self.router.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await self.connect(websocket)
            try:
                while True:
                    try:
                        data = await websocket.receive_text()
                        symbols = json.loads(data).get("params", {}).get("symbol", [])
                        await self.update_client_symbols(websocket, symbols)
                    except (WebSocketDisconnect, ConnectionClosedOK):
                        break
                    except Exception as e:
                        logger.error(
                            f"Error handling client message: {e}", exc_info=True
                        )
            finally:
                await self.disconnect(websocket)

        @self.router.get("/symbology")
        async def get_symbology(asset_type: Optional[str] = None):
            """Get the symbology map from Databento client"""
            if (
                not self.is_running
                or not self.client
                or not hasattr(self.client, "symbology_map")
            ):
                # If client isn't running yet, start it
                if not self.is_running:
                    await self.start_master_connection()

                await asyncio.sleep(1)

                # If we still don't have a symbology map, return empty
                if not self.client or not hasattr(self.client, "symbology_map"):
                    return {"status": "initializing", "symbols": {}}

            # Return the symbology map as a dictionary
            try:
                sym_map = self.client.symbology_map

                if asset_type == "index":
                    return sorted(
                        [
                            {"label": names_map.get(v[:-2], v), "value": v}
                            for v in sym_map.values()
                            if (
                                v.startswith("ES")
                                or v.startswith("MES")
                                or v.startswith("NQ")
                                or v.startswith("MNQ")
                                or v.startswith("RTY")
                                or v.startswith("YM")
                                or v.startswith("NKD")
                                or v.startswith("RX")
                                or v.startswith("AW")
                            )
                            and not v.startswith("ESR")
                        ],
                        key=lambda x: x["label"],
                    )
                if asset_type == "fx":
                    return sorted(
                        [
                            {"label": names_map.get(v[:-2], v), "value": v}
                            for v in sym_map.values()
                            if (
                                v.startswith("6")
                                or v.startswith("SIR")
                                or v.startswith("CNH")
                                or v.startswith("BTC")
                                or v.startswith("ETH")
                            )
                        ],
                        key=lambda x: x["label"],
                    )

                if asset_type == "interest_rates":
                    return sorted(
                        [
                            {"label": names_map.get(v[:-2], v), "value": v}
                            for v in sym_map.values()
                            if (
                                v.startswith("SR1")
                                or v.startswith("SR3")
                                or v.startswith("ESR")
                                or v.startswith("ZQ")
                                or v.startswith("ZB")
                                or v.startswith("UB")
                                or v.startswith("ZF")
                                or v.startswith("ZT")
                                or v.startswith("ZN")
                                or v.startswith("TN")
                            )
                        ],
                        key=lambda x: x["label"],
                    )

                if asset_type == "metals":
                    return sorted(
                        [
                            {"label": names_map.get(v[:-2], v), "value": v}
                            for v in sym_map.values()
                            if (
                                v.startswith("GC")
                                or v.startswith("QO")
                                or v.startswith("MGC")
                                or (v.startswith("SI") and not v.startswith("SIR"))
                                or v.startswith("PL")
                                or v.startswith("PA")
                                or v.startswith("HG")
                                or v.startswith("ALI")
                                or v.startswith("HRC")
                            )
                        ],
                        key=lambda x: x["label"],
                    )

                if asset_type == "agriculture":
                    return sorted(
                        [
                            {"label": names_map.get(v[:-2], v), "value": v}
                            for v in sym_map.values()
                            if (
                                v.startswith("ZW")
                                or v.startswith("ZC")
                                or v.startswith("ZM")
                                or v.startswith("ZR")
                                or v.startswith("ZS")
                                or v.startswith("ZL")
                                or v.startswith("ZO")
                                or v.startswith("KE")
                                or v.startswith("DY")
                                or v.startswith("CB")
                                or v.startswith("DC")
                                or v.startswith("CSC")
                                or v.startswith("LBR")
                                or v.startswith("HE")
                                or v.startswith("LE")
                                or v.startswith("GF")
                            )
                        ],
                        key=lambda x: x["label"],
                    )
                if asset_type == "energy":
                    return sorted(
                        [
                            {"label": names_map.get(v[:-2], v), "value": v}
                            for v in sym_map.values()
                            if (
                                v.startswith("CL")
                                or v.startswith("QM")
                                or v.startswith("BZ")
                                or v.startswith("RB")
                                or v.startswith("HO")
                                or v.startswith("NG")
                                or v.startswith("QG")
                                or v.startswith("QG")
                                or v.startswith("LTH")
                                or v.startswith("UME")
                            )
                        ],
                        key=lambda x: x["label"],
                    )
                return sorted(
                    [
                        {"label": v, "value": v}
                        for v in self.client.symbology_map.values()
                        if " " not in v
                        and "-" not in v
                        and ":" not in v
                        and "#" not in v
                    ],
                    key=lambda x: x["label"],
                )
            except Exception as e:
                logger.error(f"Error getting symbology map: {e}", exc_info=True)
                return {"status": "error", "message": str(e), "symbols": {}}

        @self.router.get("/control/subscribe")
        async def control_subscribe(symbols: str):
            """Subscribe to symbols from external control"""
            symbols = symbols.split(",") if isinstance(symbols, str) else symbols
            if not symbols:
                return {"success": False, "message": "No symbols provided"}

            # Start the master connection if not already running
            if not self.is_running:
                await self.start_master_connection()

            # Add the symbols to a special control subscriber
            for symbol in symbols:
                if symbol not in self.symbol_subscribers:
                    self.symbol_subscribers[symbol] = set()

            return {"success": True, "subscribed": symbols}

        @self.router.get("/master_stream")
        async def master_stream_endpoint():
            """Stream raw data directly from the master connection"""

            async def event_generator():
                async for message in self.get_master_stream():
                    # Format as Server-Sent Events
                    yield f"data: {json.dumps(message)}\n\n"

            return StreamingResponse(
                event_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Content-Type": "text/event-stream",
                },
            )

        @self.router.get("/control/status")
        async def control_status():
            """Get the status of the WebSocket connection"""
            return {
                "connected": self.is_running and self.client is not None,
                "active_clients": len(self.active_connections),
                "subscribed_symbols": list(self.symbol_subscribers.keys()),
            }

        @self.router.get("/get_ws_data")
        async def get_messages(symbol: str = None) -> list:
            """Get the latest data for specified symbols

            This endpoint returns the most recent message received for each symbol
            from the Databento stream. If a symbol hasn't received any messages yet,
            it returns a placeholder with default values.

            Args:
                symbol: Comma-separated list of symbols

            Returns:
                List of latest messages for each symbol
            """
            # Return empty list if there are no subscribed symbols

            symbols = symbol.split(",") if isinstance(symbol, str) and symbol else []

            # Return empty list if no symbols provided
            if not symbols:
                return []

            result = []
            current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

            with self.message_cache_lock:
                for sym in symbols:
                    # If we have cached data for this symbol, use it
                    if sym in self.message_cache:
                        result.append(self.message_cache[sym])
                    else:
                        # Otherwise return a placeholder with empty values
                        result.append(
                            {
                                "date": current_time,
                                "symbol": sym,
                                "name": names_map.get(sym[:-2], sym),
                                "size": 0,
                                "side": "",
                                "price": 0.0,
                            }
                        )

            return result

        @self.router.get("/control/unsubscribe")
        async def control_unsubscribe(symbols: str):
            """Unsubscribe from symbols from external control"""
            symbols = symbols.split(",") if isinstance(symbols, str) else symbols
            if not symbols:
                return {
                    "success": False,
                    "message": "No symbols provided",
                }

            to_unsubscribe = []
            for symbol in symbols:
                if (
                    symbol in self.symbol_subscribers
                    and not self.symbol_subscribers[symbol]
                ):
                    to_unsubscribe.append(symbol)

            # Clean up the symbol subscribers dictionary
            for symbol in to_unsubscribe:
                if (
                    symbol in self.symbol_subscribers
                    and not self.symbol_subscribers[symbol]
                ):
                    del self.symbol_subscribers[symbol]

            return {"success": True, "unsubscribed": to_unsubscribe}

    async def get_master_stream(self):
        """
        Creates an async generator that yields messages directly from the master connection.
        This can be used to create streaming endpoints.

        Yields:
            dict: Raw messages from the Databento client
        """
        if not self.is_running:
            # Start the master connection if not already running
            await self.start_master_connection()

        # Create a new queue for this stream
        stream_queue = asyncio.Queue()

        # Add this queue to master stream queues
        self.master_stream_queues.add(stream_queue)

        try:
            while self.is_running:
                try:
                    # Wait for a message to be added to this stream's queue
                    message = await asyncio.wait_for(stream_queue.get(), timeout=30.0)

                    # Parse and yield the message
                    if isinstance(message, str):
                        try:
                            yield json.loads(message)
                        except json.JSONDecodeError:
                            yield {"error": "Invalid JSON", "raw": message}
                    else:
                        yield message

                    # Mark as processed
                    stream_queue.task_done()

                except asyncio.TimeoutError:
                    # Send a heartbeat to keep the connection alive
                    yield {"type": "heartbeat", "timestamp": datetime.now().isoformat()}
                except Exception as e:
                    logger.error(f"Error in master stream: {e}", exc_info=True)
                    yield {"error": str(e)}
                    await asyncio.sleep(1)
        finally:
            # Clean up when the stream ends
            if stream_queue in self.master_stream_queues:
                self.master_stream_queues.remove(stream_queue)

    def user_callback(self, record: db.DBNRecord) -> None:
        """Callback function for Databento record handling"""
        try:
            msg = {}

            if isinstance(record, db.TradeMsg):
                # Get the symbol from symbology map
                symbol = self.client.symbology_map.get(record.instrument_id)
                name = names_map.get(symbol[:-2], symbol)
                # Create the trade message
                msg = {
                    "date": record.pretty_ts_event.tz_convert(
                        timezone("America/Chicago")
                    ).strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "symbol": symbol,
                    "name": name,
                    "side": (
                        "Ask"
                        if record.side == "A"
                        else "Bid" if record.side == "B" else "Neutral"
                    ),
                    "size": record.size,
                    "price": record.pretty_price,
                }

            if msg:
                # Update the message cache with this latest data
                with self.message_cache_lock:
                    self.message_cache[symbol] = msg

                # Add to the message queue for processing
                self.message_queue.put(msg)

                # Also directly broadcast to master stream queues
                if self.master_stream_queues and self.loop:
                    for queue in self.master_stream_queues:
                        asyncio.run_coroutine_threadsafe(queue.put(msg), self.loop)

        except Exception as e:
            logger.error(f"Error in user_callback: {e}")

    def error_handler(self, exception: Exception) -> None:
        """Error handler for Databento client"""
        logger.error(f"Databento error: {exception}")
        # Put a reconnection message in the queue
        self.message_queue.put({"_reconnect": True})

    def process_messages_thread(self):
        """Thread function to process messages from the queue"""
        asyncio.set_event_loop(self.loop)
        self.thread_running = True

        while self.thread_running:
            try:
                try:
                    message = self.message_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                # Check if this is a reconnection signal
                if isinstance(message, dict) and message.get("_reconnect"):
                    asyncio.run_coroutine_threadsafe(self.reconnect(), self.loop)
                    self.message_queue.task_done()
                    continue

                # Process message
                try:
                    # Check if this is a trade message from Databento
                    if (
                        isinstance(message, dict)
                        and "symbol" in message
                        and "price" in message
                    ):
                        symbol = message.get("symbol")
                        if symbol and symbol in self.symbol_subscribers:
                            subscribers = self.symbol_subscribers[symbol].copy()
                            if subscribers:
                                asyncio.run_coroutine_threadsafe(
                                    self.broadcast_to_subscribers(subscribers, message),
                                    self.loop,
                                )
                finally:
                    # Mark the message as processed
                    self.message_queue.task_done()
            except Exception as e:
                logger.error(f"Error in message processor thread: {e}")

    async def broadcast_to_subscribers(self, subscribers: set[WebSocket], msg: dict):
        """Broadcast a message to all specified subscribers"""
        for client in subscribers:
            try:
                await client.send_json(msg)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")

    async def start_master_connection(self):
        """Start the Databento Live client connection"""
        # Don't reconnect if already running
        if self.is_running and self.client:
            return

        # Clean up any existing connection first
        await self.stop_master_connection()

        retries = 0
        while retries <= self.max_retries:
            try:
                logger.info(
                    f"Starting Databento client (attempt {retries + 1}/{self.max_retries + 1})"
                )

                # Initialize the Databento client
                self.client = db.Live(self.api_key)

                # Store the event loop for the thread to use
                self.loop = asyncio.get_event_loop()

                # Start thread to process messages from the queue
                self.thread_running = True
                self.processor_thread = threading.Thread(
                    target=self.process_messages_thread, daemon=True
                )
                self.processor_thread.start()

                # Subscribe to all symbols
                self.client.subscribe(
                    dataset="GLBX.MDP3",
                    schema="trades",
                    stype_in="continuous",
                    symbols=[f"{d}.c.0" for d in names_map.keys()],
                )

                # Add callback function
                self.client.add_callback(
                    record_callback=self.user_callback,
                    exception_callback=self.error_handler,
                )

                # Start the client
                self.is_running = True
                self.client.start()

                logger.info("Databento master connection established")

                # Connection successful, exit retry loop
                break

            except Exception as e:
                retries += 1
                if retries <= self.max_retries:
                    logger.warning(
                        f"Connection error: {e}, retrying in {self.retry_delay} seconds..."
                    )
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"Failed to establish Databento connection: {e}", exc_info=True
                    )
                    self.is_running = False
                    break

    async def stop_master_connection(self):
        """Stop the Databento client connection"""
        self.is_running = False

        # Stop the message processing thread
        if self.processor_thread and self.processor_thread.is_alive():
            self.thread_running = False
            self.processor_thread.join(timeout=2.0)
            self.processor_thread = None

        # Stop Databento client
        if self.client:
            try:
                self.client.stop()
            except Exception as e:
                logger.error(f"Error stopping Databento client: {e}")
            self.client = None

        # Clear the message queue
        while True:
            try:
                self.message_queue.get_nowait()
                self.message_queue.task_done()
            except queue.Empty:
                break

        logger.info("Databento master connection closed")

    async def reconnect(self):
        """Handle reconnection after a connection failure"""
        if not self.is_running:
            return

        self.is_running = False

        # Wait a bit before reconnecting
        await asyncio.sleep(self.retry_delay)

        # Always reconnect, regardless of client connections
        await self.start_master_connection()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        self.client_subscriptions[websocket] = set()

        # Start the master connection if not already running
        if not self.is_running:
            await self.start_master_connection()

    async def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

            # Remove client subscriptions
            if websocket in self.client_subscriptions:
                subscribed_symbols = self.client_subscriptions.pop(websocket)

                # Update symbol subscribers
                for symbol in subscribed_symbols:
                    if symbol in self.symbol_subscribers:
                        self.symbol_subscribers[symbol].discard(websocket)

            if websocket.application_state == 1:
                await websocket.close(reason="Client disconnected")

    async def update_client_symbols(self, websocket: WebSocket, symbols: list):
        """Update the symbols a client is subscribed to"""
        async with self.lock:
            if websocket not in self.active_connections:
                return

            old_symbols = self.client_subscriptions.get(websocket, set())
            new_symbols = set(symbols)

            # Symbols to subscribe to
            to_subscribe = new_symbols - old_symbols

            # Symbols to unsubscribe from
            to_unsubscribe = old_symbols - new_symbols

            # Update client subscriptions
            self.client_subscriptions[websocket] = new_symbols

            # Update symbol subscribers
            for symbol in to_subscribe:
                if symbol not in self.symbol_subscribers:
                    self.symbol_subscribers[symbol] = set()
                self.symbol_subscribers[symbol].add(websocket)

            for symbol in to_unsubscribe:
                if symbol in self.symbol_subscribers:
                    self.symbol_subscribers[symbol].discard(websocket)


def create_databento_manager(api_key: str):
    """Create a ConnectionManager configured for Databento Live API"""
    return ConnectionManager(
        api_key=api_key,
        connection_timeout=30.0,
        retry_delay=5.0,
        max_retries=3,
    )
