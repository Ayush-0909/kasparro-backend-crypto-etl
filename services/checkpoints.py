from datetime import datetime
from core.database import SessionLocal
from schemas.crypto import ETLCheckpoint


def get_checkpoint(source: str):
    db = SessionLocal()
    checkpoint = db.query(ETLCheckpoint).filter_by(source=source).first()
    db.close()
    return checkpoint


def update_checkpoint(source: str):
    db = SessionLocal()
    checkpoint = db.query(ETLCheckpoint).filter_by(source=source).first()

    if checkpoint:
        checkpoint.last_processed_at = datetime.utcnow()
    else:
        checkpoint = ETLCheckpoint(
            source=source,
            last_processed_at=datetime.utcnow()
        )
        db.add(checkpoint)

    db.commit()
    db.close()
