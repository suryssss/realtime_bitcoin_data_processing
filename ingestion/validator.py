# establishing rules to only add valid records to kafka 

'''
rules:
    trade_id: non-null string
    product_id: supported product
    price: positive finite number
    quantity: positive finite number
    side: either BUY or SELL
    trade_time: valid ISO 8601 timestamp with timezone
'''

from decimal import Decimal, InvalidOperation
from datetime import datetime

VALID_PRODUCTS = {"BTC-USD"}
VALID_SIDES = {"BUY", "SELL"}


def validate_trade(trade):
    errors = []

    # 1. Validate trade_id
    trade_id = trade.get("trade_id")
    if not isinstance(trade_id, str) or not trade_id.strip():
        errors.append("Invalid trade_id")

    # 2. Validate product_id
    product_id = trade.get("product_id")
    if product_id not in VALID_PRODUCTS:
        errors.append("Unsupported product_id")

    # 3. Validate price
    try:
        price = Decimal(str(trade.get("price")))
        if not price.is_finite() or price <= 0:
            errors.append("Price must be finite and positive")
    except (InvalidOperation, TypeError, ValueError):
        errors.append("Invalid Price")

    # 4. Validate quantity
    try:
        quantity = Decimal(str(trade.get("quantity")))
        if not quantity.is_finite() or quantity <= 0:
            errors.append("Quantity must be finite and positive")
    except (InvalidOperation, TypeError, ValueError):
        errors.append("Invalid Quantity")

    # 5. Validate side
    side = str(trade.get("side", "")).upper()
    if side not in VALID_SIDES:
        errors.append("Unsupported side")

    # 6. Validate trade_time
    trade_time = trade.get("trade_time") or trade.get("time")
    try:
        if not isinstance(trade_time, str):
            raise ValueError("Timestamp must be a string")

        parsed_time = datetime.fromisoformat(
            trade_time.replace("Z", "+00:00")
        )

        if parsed_time.tzinfo is None:
            raise ValueError("Timestamp must include timezone")

    except (ValueError, TypeError):
        errors.append("Invalid trade timestamp")

    if errors:
        return False, errors
    
    return True, []