from datetime import datetime
from sqlalchemy.orm import Session
from schemas.crypto import ETLCheckpoint


def get_checkpoint(db: Session, source: str):
    checkpoint = (
        db.query(ETLCheckpoint)
        .filter(ETLCheckpoint.source == source)
        .first()
    )

    if not checkpoint:
        checkpoint = ETLCheckpoint(
            source=source,
            last_processed_at=None
        )
        db.add(checkpoint)
        db.commit()
        db.refresh(checkpoint)

    return checkpoint


def update_checkpoint(db: Session, source: str):
    checkpoint = (
        db.query(ETLCheckpoint)
        .filter(ETLCheckpoint.source == source)
        .first()
    )

    if checkpoint:
        checkpoint.last_processed_at = datetime.utcnow()
    else:
        checkpoint = ETLCheckpoint(
            source=source,
            last_processed_at=datetime.utcnow()
        )
        db.add(checkpoint)

    db.commit()
