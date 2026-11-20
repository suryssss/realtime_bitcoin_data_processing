import sys
import unittest
from pathlib import Path

# Ensure ingestion package is in path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR / "ingestion"))

from validator import validate_trade


class TestTradeValidator(unittest.TestCase):

    def test_valid_trade(self):
        trade = {
            "trade_id": "12345678",
            "product_id": "BTC-USD",
            "price": "65000.50",
            "quantity": "0.15000000",
            "side": "BUY",
            "trade_time": "2026-11-20T10:00:00.000Z",
            "exchange": "COINBASE"
        }
        is_valid, errors = validate_trade(trade)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)

    def test_invalid_price(self):
        trade = {
            "trade_id": "12345679",
            "product_id": "BTC-USD",
            "price": "-100.00",
            "quantity": "0.1",
            "side": "BUY",
            "trade_time": "2026-11-20T10:00:00.000Z"
        }
        is_valid, errors = validate_trade(trade)
        self.assertFalse(is_valid)
        self.assertTrue(any("Price" in err for err in errors))

    def test_invalid_quantity(self):
        trade = {
            "trade_id": "12345680",
            "product_id": "BTC-USD",
            "price": "65000.00",
            "quantity": "0",
            "side": "SELL",
            "trade_time": "2026-11-20T10:00:00.000Z"
        }
        is_valid, errors = validate_trade(trade)
        self.assertFalse(is_valid)
        self.assertTrue(any("Quantity" in err for err in errors))

    def test_invalid_side(self):
        trade = {
            "trade_id": "12345681",
            "product_id": "BTC-USD",
            "price": "65000.00",
            "quantity": "0.5",
            "side": "HOLD",
            "trade_time": "2026-11-20T10:00:00.000Z"
        }
        is_valid, errors = validate_trade(trade)
        self.assertFalse(is_valid)
        self.assertTrue(any("side" in err.lower() for err in errors))

    def test_missing_trade_id(self):
        trade = {
            "trade_id": "   ",
            "product_id": "BTC-USD",
            "price": "65000.00",
            "quantity": "0.5",
            "side": "BUY",
            "trade_time": "2026-11-20T10:00:00.000Z"
        }
        is_valid, errors = validate_trade(trade)
        self.assertFalse(is_valid)
        self.assertTrue(any("trade_id" in err for err in errors))

    def test_multi_product_elasticity(self):
        eth_trade = {
            "trade_id": "eth-9999",
            "product_id": "ETH-USD",
            "price": "3450.25",
            "quantity": "2.5",
            "side": "BUY",
            "trade_time": "2026-11-20T10:00:00.000Z"
        }
        # Explicit allowed set
        is_valid, errors = validate_trade(eth_trade, allowed_products={"BTC-USD", "ETH-USD"})
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)

        # Unsupported product test
        doge_trade = dict(eth_trade, product_id="DOGE-USD")
        is_valid, errors = validate_trade(doge_trade, allowed_products={"BTC-USD", "ETH-USD"})
        self.assertFalse(is_valid)
        self.assertTrue(any("Unsupported product_id" in err for err in errors))


if __name__ == "__main__":
    unittest.main()
