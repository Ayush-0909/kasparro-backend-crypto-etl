from core.database import Base, engine

# IMPORT ALL MODELS HERE
from schemas.crypto import (
    RawCryptoCSV,
    RawCoinPaprika,
    RawCoinGecko,
    CryptoAsset,
    ETLRun,
    ETLCheckpoint,
)

def init_db():
    Base.metadata.create_all(bind=engine)
