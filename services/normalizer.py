from datetime import datetime
import json
from sqlalchemy.orm import Session

from schemas.crypto import (
    RawCryptoCSV,
    RawCoinPaprika,
    RawCoinGecko,
    CryptoAsset,
)
from services.checkpoints import get_checkpoint, update_checkpoint


def normalize_csv(db: Session):
    rows = db.query(RawCryptoCSV).all()
    if not rows:
        return

    for r in rows:
        asset = CryptoAsset(
            coin_name=r.coin_name,
            symbol=r.symbol,
            price_usd=r.price_usd,
            market_cap=r.market_cap,
            volume_24h=r.volume_24h,
            source="csv",
            last_updated=r.last_updated,
        )
        db.add(asset)

    update_checkpoint(db, "csv", datetime.utcnow())
    db.commit()


def normalize_coinpaprika(db: Session):
    rows = db.query(RawCoinPaprika).all()
    if not rows:
        return

    data = json.loads(rows[-1].payload)

    for c in data:
        asset = CryptoAsset(
            coin_name=c.get("name"),
            symbol=c.get("symbol"),
            price_usd=None,
            market_cap=None,
            volume_24h=None,
            source="coinpaprika",
            last_updated=datetime.utcnow(),
        )
        db.add(asset)

    update_checkpoint(db, "coinpaprika", datetime.utcnow())
    db.commit()


def normalize_coingecko(db: Session):
    rows = db.query(RawCoinGecko).all()
    if not rows:
        return

    data = json.loads(rows[-1].payload)

    for c in data:
        asset = CryptoAsset(
            coin_name=c.get("name"),
            symbol=c.get("symbol"),
            price_usd=c.get("current_price"),
            market_cap=c.get("market_cap"),
            volume_24h=c.get("total_volume"),
            source="coingecko",
            last_updated=datetime.utcnow(),
        )
        db.add(asset)

    update_checkpoint(db, "coingecko", datetime.utcnow())
    db.commit()
