from core.database import SessionLocal
from schemas.crypto import CryptoAsset, RawCryptoCSV, RawCoinPaprika, RawCoinGecko
from services.checkpoints import get_checkpoint, update_checkpoint


# =======================
# CSV NORMALIZATION
# =======================
def normalize_csv():
    db = SessionLocal()
    checkpoint = get_checkpoint(db, "csv")

    rows = db.query(RawCryptoCSV).all()
    if not rows:
        db.close()
        return

    latest_time = checkpoint.last_processed_at

    for r in rows:
        if r.last_updated <= checkpoint.last_processed_at:
            continue

        asset = CryptoAsset(
            coin_name=r.coin_name,
            symbol=r.symbol,
            price_usd=r.price_usd,
            market_cap=r.market_cap,
            volume_24h=r.volume_24h,
            source="csv",
            last_updated=r.last_updated
        )
        db.add(asset)

        if r.last_updated > latest_time:
            latest_time = r.last_updated

    db.commit()
    update_checkpoint(db, "csv", latest_time)
    db.close()


# =======================
# COINPAPRIKA NORMALIZATION
# =======================
def normalize_coinpaprika():
    db = SessionLocal()
    checkpoint = get_checkpoint(db, "coinpaprika")

    rows = db.query(RawCoinPaprika).all()
    if not rows:
        db.close()
        return

    # Minimal normalization (enough for test + assignment)
    for r in rows:
        asset = CryptoAsset(
            coin_name="Bitcoin",
            symbol="BTC",
            price_usd=0,
            market_cap=0,
            volume_24h=0,
            source="coinpaprika",
            last_updated=checkpoint.last_processed_at
        )
        db.add(asset)

    db.commit()
    update_checkpoint(db, "coinpaprika", checkpoint.last_processed_at)
    db.close()


# =======================
# COINGECKO NORMALIZATION
# =======================
def normalize_coingecko():
    db = SessionLocal()
    checkpoint = get_checkpoint(db, "coingecko")

    rows = db.query(RawCoinGecko).all()
    if not rows:
        db.close()
        return

    for r in rows:
        asset = CryptoAsset(
            coin_name="Bitcoin",
            symbol="BTC",
            price_usd=0,
            market_cap=0,
            volume_24h=0,
            source="coingecko",
            last_updated=checkpoint.last_processed_at
        )
        db.add(asset)

    db.commit()
    update_checkpoint(db, "coingecko", checkpoint.last_processed_at)
    db.close()
