import json 


def parse_market_trade(message):
    
    data=json.load(message)

    #ignore messages from other channels
    if data.get("channel") !="market_trades":
        return []
    
    normalised_trades=[]

    #a message can contain multiple events:
    for events in data.get("events",[]):
        event_type=event.get("type")

        if event_type not in ["snapshot","update"]:
            continue

        #each event can contain multiple trades
        for trade in event.get("trades",[]):

            normalised_trade={
                "trade_id":str(trade.get("trade_id")),
                "product_id":trade.get("product_id"),
                "price":trade.get("price"),
                "quantity":trade.get("quantity"),
                "side":trade.get("side"),
                "trade_time":trade.get("time")     
            }

            normalised_trades.append(normalised_trade)

        return normalised_trades