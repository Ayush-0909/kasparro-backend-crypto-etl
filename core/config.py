import os
from dotenv import load_dotenv

load_dotenv()

# =========================
# DATABASE
# =========================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///./app.db"  # local fallback
)

# =========================
# API URLs
# =========================
COINPAPRIKA_API_URL = "https://api.coinpaprika.com/v1/tickers"
COINGECKO_API_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=10&page=1"
)

# =========================
# API KEYS (if needed later)
# =========================
COIN_API_KEY = os.getenv("COIN_API_KEY")
