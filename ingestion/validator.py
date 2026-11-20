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

import os
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def get_supported_products():
    """Load supported cryptocurrency pairs from environment or fallback to BTC-USD."""
    env_products = os.getenv("PRODUCTS") or os.getenv("PRODUCT_IDS", "BTC-USD")
    return {p.strip().upper() for p in env_products.split(",") if p.strip()}


VALID_PRODUCTS = get_supported_products()
VALID_SIDES = {"BUY", "SELL"}


def validate_trade(trade, allowed_products=None):
    errors = []
    supported_products = allowed_products if allowed_products is not None else get_supported_products()

    # 1. Validate trade_id
    trade_id = trade.get("trade_id")
    if not isinstance(trade_id, str) or not trade_id.strip():
        errors.append("Invalid trade_id")

    # 2. Validate product_id
    product_id = trade.get("product_id")
    if product_id not in supported_products:
        errors.append(f"Unsupported product_id: {product_id}")

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