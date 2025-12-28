from datetime import datetime
from sqlalchemy.orm import Session
from schemas.crypto import ETLCheckpoint


def get_checkpoint(db: Session, source: str):
    return db.query(ETLCheckpoint).filter_by(source=source).first()


def update_checkpoint(db: Session, source: str, last_processed_at: datetime):
    checkpoint = get_checkpoint(db, source)

    if checkpoint:
        checkpoint.last_processed_at = last_processed_at
    else:
        checkpoint = ETLCheckpoint(
            source=source,
            last_processed_at=last_processed_at,
        )
        db.add(checkpoint)
