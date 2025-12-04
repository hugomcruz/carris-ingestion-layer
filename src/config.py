"""Configuration management using Pydantic Settings"""

from urllib.parse import quote_plus
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    # GTFS API Configuration
    gtfs_api_url: str = "https://gateway.carris.pt/gateway/gtfs/api/v2.11/GTFS/realtime/vehiclepositions"
    poll_interval_seconds: int = 30
    
    # Redis Configuration
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = ""
    redis_db: int = 0
    max_concurrent_redis_operations: int = 100  # Max parallel Redis operations
    
    # Vehicle Inactivity Timeout
    vehicle_inactivity_timeout_seconds: int = 180  # 3 minutes
    
    # Database Configuration (GTFS Static Data)
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "carris_gtfs"
    db_user: str = "carris"
    db_password: str = ""
    gtfs_refresh_hour: int = 4  # Hour to refresh GTFS data (0-23)
    
    # Application
    app_name: str = "Carris Vehicle Ingestion"
    log_level: str = "INFO"
    
    @property
    def redis_url(self) -> str:
        """Construct Redis URL from components"""
        if self.redis_password:
            encoded_password = quote_plus(self.redis_password)
            return f"redis://:{encoded_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    @property
    def db_url(self) -> str:
        """Construct PostgreSQL database URL"""
        encoded_user = quote_plus(self.db_user)
        encoded_password = quote_plus(self.db_password)
        return f"postgresql+asyncpg://{encoded_user}:{encoded_password}@{self.db_host}:{self.db_port}/{self.db_name}"


# Global settings instance
settings = Settings()
