from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
COIN_API_KEY = os.getenv("COIN_API_KEY")
