import json
from core.database import SessionLocal
from schemas.crypto import (
    RawCryptoCSV,
    RawCoinPaprika,
    RawCoinGecko,
    CryptoAsset
)
from services.checkpoints import get_checkpoint, update_checkpoint


# =========================
# CSV NORMALIZATION
# =========================
def normalize_csv():
    db = SessionLocal()

    try:
        checkpoint = get_checkpoint("csv")
        query = db.query(RawCryptoCSV)

        if checkpoint:
            query = query.filter(
                RawCryptoCSV.ingested_at > checkpoint.last_processed_at
            )

        rows = query.all()

        for row in rows:
            asset = CryptoAsset(
                coin_name=row.coin_name,
                symbol=row.symbol,
                price_usd=row.price_usd,
                market_cap=row.market_cap,
                volume_24h=row.volume_24h,
                source="csv",
                last_updated=row.last_updated
            )
            db.add(asset)

        db.commit()

        if rows:
            update_checkpoint("csv")

    except Exception as e:
        db.rollback()
        print("❌ CSV normalization failed:", e)

    finally:
        db.close()


# =========================
# COINPAPRIKA NORMALIZATION
# =========================
def normalize_coinpaprika():
    db = SessionLocal()

    try:
        checkpoint = get_checkpoint("coinpaprika")
        query = db.query(RawCoinPaprika)

        if checkpoint:
            query = query.filter(
                RawCoinPaprika.ingested_at > checkpoint.last_processed_at
            )

        records = query.all()

        for record in records:
            data = json.loads(record.payload)

            for coin in data:
                asset = CryptoAsset(
                    coin_name=coin.get("name"),
                    symbol=coin.get("symbol"),
                    price_usd=coin.get("quotes", {}).get("USD", {}).get("price"),
                    market_cap=coin.get("quotes", {}).get("USD", {}).get("market_cap"),
                    volume_24h=coin.get("quotes", {}).get("USD", {}).get("volume_24h"),
                    source="coinpaprika",
                    last_updated=None
                )
                db.add(asset)

        db.commit()

        if records:
            update_checkpoint("coinpaprika")

    except Exception as e:
        db.rollback()
        print("❌ CoinPaprika normalization failed:", e)

    finally:
        db.close()


# =========================
# COINGECKO NORMALIZATION
# =========================
def normalize_coingecko():
    db = SessionLocal()

    try:
        checkpoint = get_checkpoint("coingecko")
        query = db.query(RawCoinGecko)

        if checkpoint:
            query = query.filter(
                RawCoinGecko.ingested_at > checkpoint.last_processed_at
            )

        records = query.all()

        for record in records:
            data = json.loads(record.payload)

            for coin in data:
                asset = CryptoAsset(
                    coin_name=coin.get("name"),
                    symbol=coin.get("symbol"),
                    price_usd=coin.get("current_price"),
                    market_cap=coin.get("market_cap"),
                    volume_24h=coin.get("total_volume"),
                    source="coingecko",
                    last_updated=None
                )
                db.add(asset)

        db.commit()

        if records:
            update_checkpoint("coingecko")

    except Exception as e:
        db.rollback()
        print("❌ CoinGecko normalization failed:", e)

    finally:
        db.close()
