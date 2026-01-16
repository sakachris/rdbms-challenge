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

    # 🔥 This is the important part
    # Local → ./data
    # Docker/K8s → /app/data
    # DATABASE_DIR = os.getenv(
    #     "DATABASE_DIR",
    #     str(BASE_DIR / "data")
    # )
    DATABASE_DIR = Path(
        os.getenv("DATABASE_DIR", BASE_DIR / "data")
    )

    # Server
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "8000"))



# """Configuration for SimplDB Blog"""
# import os

# BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# class Config:
#     """Application configuration"""
#     SECRET_KEY = os.environ.get('SECRET_KEY') or 'simpldb-secret-key-change-in-production'
#     DATABASE_NAME = 'blogdb'
#     DATABASE_DIR = os.path.join(BASE_DIR, 'data')
#     DEBUG = True
#     HOST = '0.0.0.0'
#     PORT = 5000
