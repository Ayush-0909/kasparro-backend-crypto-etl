from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

# -----------------------------
# RAW TABLES
# -----------------------------

class RawCoinPaprika(Base):
    __tablename__ = "raw_coinpaprika"

    id = Column(Integer, primary_key=True, index=True)
    data = Column(Text)
    fetched_at = Column(DateTime, default=datetime.utcnow)


class RawCoinGecko(Base):
    __tablename__ = "raw_coingecko"

    id = Column(Integer, primary_key=True, index=True)
    data = Column(Text)
    fetched_at = Column(DateTime, default=datetime.utcnow)


class RawCSV(Base):
    __tablename__ = "raw_csv"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String)
    category = Column(String)
    fetched_at = Column(DateTime, default=datetime.utcnow)

# -----------------------------
# UNIFIED NORMALIZED TABLE
# -----------------------------

class CryptoAsset(Base):
    __tablename__ = "crypto_assets"

    id = Column(Integer, primary_key=True, index=True)
    coin_name = Column(String, index=True)
    symbol = Column(String, index=True)
    price_usd = Column(Float)
    market_cap = Column(Float)
    volume_24h = Column(Float)
    source = Column(String)
    last_updated = Column(DateTime, default=datetime.utcnow)

# -----------------------------
# ETL CHECKPOINT TABLE
# -----------------------------

class ETLCheckpoint(Base):
    __tablename__ = "etl_checkpoints"

    source = Column(String, primary_key=True)
    last_processed = Column(DateTime)
    status = Column(String)

# -----------------------------
# ETL RUN METADATA (P2)
# -----------------------------

class ETLRun(Base):
    __tablename__ = "etl_runs"

    id = Column(Integer, primary_key=True, index=True)
    records_processed = Column(Integer)
    duration_sec = Column(Float)
    status = Column(String)
    run_time = Column(DateTime, default=datetime.utcnow)
