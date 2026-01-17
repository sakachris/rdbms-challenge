"""Configuration for SimplDB Blog"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Base directory of webapp/
BASE_DIR = Path(__file__).resolve().parent


class Config:
    """Application configuration"""

    # Security
    SECRET_KEY = os.getenv("SECRET_KEY")
    if not SECRET_KEY:
        raise RuntimeError("SECRET_KEY not set")

    # Database
    DATABASE_NAME = os.getenv("DATABASE_NAME")

    # Local → ./data
    # Docker/K8s → /app/data
    DATABASE_DIR = Path(os.getenv("DATABASE_DIR", BASE_DIR / "data"))

    # Server
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "8000"))
