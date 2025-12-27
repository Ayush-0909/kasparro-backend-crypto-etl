from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from core.database import Base

class RawCryptoCSV(Base):
    __tablename__ = "raw_crypto_csv"
    id = Column(Integer, primary_key=True)
    coin_name = Column(String)
    symbol = Column(String)
    price_usd = Column(Float)
    market_cap = Column(Float)
    volume_24h = Column(Float)
    last_updated = Column(DateTime)
    ingested_at = Column(DateTime, default=datetime.utcnow)

class RawCoinPaprika(Base):
    __tablename__ = "raw_coinpaprika"
    id = Column(Integer, primary_key=True)
    payload = Column(String)
    ingested_at = Column(DateTime, default=datetime.utcnow)

class RawCoinGecko(Base):
    __tablename__ = "raw_coingecko"
    id = Column(Integer, primary_key=True)
    payload = Column(String)
    ingested_at = Column(DateTime, default=datetime.utcnow)

class CryptoAsset(Base):
    __tablename__ = "crypto_assets"
    id = Column(Integer, primary_key=True)
    coin_name = Column(String)
    symbol = Column(String)
    price_usd = Column(Float)
    market_cap = Column(Float)
    volume_24h = Column(Float)
    source = Column(String)
    last_updated = Column(DateTime)

class ETLRun(Base):
    __tablename__ = "etl_runs"
    id = Column(Integer, primary_key=True)
    records_processed = Column(Integer)
    duration_sec = Column(Integer)
    status = Column(String)
    run_time = Column(DateTime, default=datetime.utcnow)
# =========================
# ETL CHECKPOINT TABLE
# =========================
class ETLCheckpoint(Base):
    __tablename__ = "etl_checkpoints"

    id = Column(Integer, primary_key=True)
    source = Column(String, unique=True, nullable=False)
    last_processed_at = Column(DateTime, default=datetime.min)
