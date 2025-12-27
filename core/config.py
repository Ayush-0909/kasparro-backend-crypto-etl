from core.database import engine
from schemas.crypto import Base

def init_db():
    """
    Create all database tables.
    """
    Base.metadata.create_all(bind=engine)
