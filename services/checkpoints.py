from datetime import datetime
from sqlalchemy.orm import Session
from schemas.crypto import ETLCheckpoint


def get_checkpoint(db: Session, source: str) -> ETLCheckpoint:
    checkpoint = db.query(ETLCheckpoint).filter_by(source=source).first()
    if not checkpoint:
        checkpoint = ETLCheckpoint(
            source=source,
            last_processed_at=datetime.min  # 👈 VERY IMPORTANT
        )
        db.add(checkpoint)
        db.commit()
        db.refresh(checkpoint)
    return checkpoint


def update_checkpoint(db: Session, source: str, last_processed_at: datetime):
    checkpoint = get_checkpoint(db, source)
    checkpoint.last_processed_at = last_processed_at
    db.commit()
