import asyncio
import json
import websockets

from event_parser import parse_market_trade
from validator import validate_trade
from producer import (
    create_producer,
    publish_trade,
    create_avro_serializer
)

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_IDS = ["BTC-USD"]

INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 60


async def connect_to_coinbase(producer, avro_serializer):

    retry_delay = INITIAL_RETRY_DELAY

    while True:
        try:
            print("\nConnecting to Coinbase WebSockets...")

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

                print(f"Subscribed to {PRODUCT_IDS}")

                # Reset retry delay after a successful connection
                retry_delay = INITIAL_RETRY_DELAY

                async for message in websocket:

                    trades = parse_market_trade(message)

                    for trade in trades:

                        is_valid, errors = validate_trade(trade)

                        if is_valid:
                            print("\nValid Trade:")
                            print(json.dumps(trade, indent=4))

                            try:
                                publish_trade(
                                    producer,
                                    avro_serializer,
                                    trade
                                )

                            except Exception as pe:
                                print(f"Kafka publishing error: {pe}")

                        else:
                            print("\nInvalid Trade:", trade)
                            print("Errors:", errors)

        except websockets.ConnectionClosed as e:
            print(f"WebSocket connection closed: {e}")

        except asyncio.TimeoutError:
            print("WebSocket connection timed out.")

        except Exception as e:
            print(f"Unexpected WebSocket error: {e}")

        print(f"Reconnecting in {retry_delay} seconds...")

        await asyncio.sleep(retry_delay)

        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)


if __name__ == "__main__":

    producer = create_producer()
    avro_serializer = create_avro_serializer()

    try:
        asyncio.run(
            connect_to_coinbase(producer, avro_serializer)
        )

    except KeyboardInterrupt:
        print("\nStopping WebSocket client...")

    finally:
        remaining = producer.flush(10)

        if remaining:
            print(f"{remaining} messages were not delivered.")
        else:
            print("All pending messages delivered.")