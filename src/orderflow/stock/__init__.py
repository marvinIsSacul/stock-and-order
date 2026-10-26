"""Stock component: products, their prices and stock levels.

Stock levels follow accepted orders through the event log. Other components only call get_prices.
"""

from orderflow.stock.service import get_prices

__all__ = ["get_prices"]
