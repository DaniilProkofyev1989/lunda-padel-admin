import os
from dotenv import load_dotenv

load_dotenv()

# Database (Neon PostgreSQL)
DATABASE_URL = os.getenv("DATABASE_URL", "")
