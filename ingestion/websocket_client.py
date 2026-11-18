import asyncio
import json
import websockets

from event_parser import parse_market_trade
from validator import validate_trade 
from producer import create_producer, publish_trade

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_IDS = ["BTC-USD"]


async def connect_to_coinbase(producer):
    while True:
        try:
            print("Connecting to Coinbase Websockets...")
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20
            ) as websocket:
                print("Connected to Coinbase!")

                subscription = {
                    "type": "subscribe",
                    "product_ids": PRODUCT_IDS,
                    "channel": "market_trades"
                }
                await websocket.send(json.dumps(subscription))
                print(f"Subscribed to {PRODUCT_IDS}. Waiting for live trades...")

                async for message in websocket:
                    trades = parse_market_trade(message)

                    for trade in trades:
                        is_valid, errors = validate_trade(trade)

                        if is_valid:
                            print("\nValid Trade:")
                            print(json.dumps(trade, indent=4))
                            try:
                                publish_trade(producer, trade)
                            except Exception as pe:
                                print(f"Error publishing to Kafka: {pe}")
                        else:
                            print("\nInvalid Trade:", trade)
                            print("Errors:", errors)

        except (websockets.ConnectionClosed, asyncio.TimeoutError) as ce:
            print(f"Websocket connection lost ({ce}). Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Websocket error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    producer = create_producer()
    try:
        asyncio.run(connect_to_coinbase(producer))
    except KeyboardInterrupt:
        print("\nStopping websocket client...")
    finally:
        print("Flushing Kafka producer...")
        producer.flush(10)
        print("Done.")