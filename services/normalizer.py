from sqlalchemy.orm import Session
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
def normalize_csv(db: Session):
    checkpoint = get_checkpoint(db, "csv")

    rows = db.query(RawCryptoCSV).all()
    if not rows:
        return

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

    update_checkpoint(db, "csv")
    db.commit()


# =========================
# COINPAPRIKA NORMALIZATION
# =========================
def normalize_coinpaprika(db: Session):
    checkpoint = get_checkpoint(db, "coinpaprika")

    rows = db.query(RawCoinPaprika).all()
    if not rows:
        return

    for row in rows:
        # payload is stored as string, skip deep parsing for now
        asset = CryptoAsset(
            coin_name="coinpaprika_asset",
            symbol="CP",
            price_usd=None,
            market_cap=None,
            volume_24h=None,
            source="coinpaprika",
            last_updated=None
        )
        db.add(asset)

    update_checkpoint(db, "coinpaprika")
    db.commit()


# =========================
# COINGECKO NORMALIZATION
# =========================
def normalize_coingecko(db: Session):
    checkpoint = get_checkpoint(db, "coingecko")

    rows = db.query(RawCoinGecko).all()
    if not rows:
        return

    for row in rows:
        asset = CryptoAsset(
            coin_name="coingecko_asset",
            symbol="CG",
            price_usd=None,
            market_cap=None,
            volume_24h=None,
            source="coingecko",
            last_updated=None
        )
        db.add(asset)

    update_checkpoint(db, "coingecko")
    db.commit()
