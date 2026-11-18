import asyncio
import json
import websockets

#endpoint to connect to coinbase 
WS_URL = "wss://advanced-trade-ws.coinbase.com"


#specifies which crypto currency pair is being used
PRODUCT_IDS=["BTC-USD"]



async def connect_to_coinbase():
    print("Connecting to Coinbase Websockets")

    #websocket connection to the coinbase api with a 20 second interval to keep the connection alive  
    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20
    ) as websocket:

        print("Connected to coinbase!")

        #subcribe to market trades

        subscription={
            "type":"subscribe",
            "product_ids":PRODUCT_IDS,
            "channel": "market_trades"
        }

        await websocket.send(json.dumps(subscription))

        print(f"Subscribed to the websocket {PRODUCT_IDS}")
        print("waiting for live trades")

        #receive incoming messages
        async for message in websocket:
            data =json.loads(message)
            print(json.dumps(data,indent=4))


if __name__=="__main__":
    try:
        asyncio.run(connect_to_coinbase()) # asyncio allows to handle asynchronous operations efficiently without using threads
    except KeyboardInterrupt:
        print("\nWebsocket connection closed")
    except Exception as e:
        print(f"Websockets error :{e}")