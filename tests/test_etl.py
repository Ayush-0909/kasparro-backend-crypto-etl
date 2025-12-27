from services.etl_service import run_etl
from core.database import SessionLocal
from schemas.crypto import CryptoAsset


def test_etl_runs_and_inserts_data():
    run_etl()

    db = SessionLocal()
    count = db.query(CryptoAsset).count()
    db.close()

    assert count > 0
