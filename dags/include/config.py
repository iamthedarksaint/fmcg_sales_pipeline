from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    # Database configuration
    DB_HOST: str
    DB_PORT: str
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str
    DB_URL: str 

    # File paths
    RAW_DATA_PATH: str
    PROCESSED_DATA_PATH: str

    model_config = SettingsConfigDict(
        env_file = ".env", env_file_encoding = "utf-8", case_sensitive = False, extra="ignore")
    
config = Settings()