import csv
from datetime import datetime
from core.database import SessionLocal
from schemas.crypto import RawCryptoCSVExtra


def ingest_csv_extra():
    db = SessionLocal()

    try:
        with open("data/crypto_extra.csv", newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                record = RawCryptoCSVExtra(
                    coin=row["coin"],
                    code=row["code"],
                    usd_price=float(row["usd_price"]),
                    marketcap=float(row["marketcap"]),
                    volume=float(row["volume"]),
                    last_seen=datetime.fromisoformat(row["last_seen"])
                )
                db.add(record)

        db.commit()

    except Exception as e:
        db.rollback()
        print("❌ Extra CSV ingestion failed:", e)

    finally:
        db.close()
