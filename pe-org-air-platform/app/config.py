from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ROOT_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # App
    app_name: str = "pe-org-air-platform"
    app_env: str = "dev"
    log_level: str = "INFO"
    api_prefix: str = "/api/v1"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_ttl_seconds: int = 300
    redis_ttl_company_seconds: int = 300
    redis_ttl_industries_seconds: int = 3600
    redis_ttl_assessment_seconds: int = 120
    redis_ttl_dimension_weights_seconds: int = 86400

    # Snowflake
    snowflake_account: str | None = None
    snowflake_user: str | None = None
    snowflake_password: str | None = None
    snowflake_warehouse: str | None = None
    snowflake_database: str | None = None
    snowflake_schema: str = "PUBLIC"
    snowflake_role: str | None = None

    # AWS / S3
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    s3_bucket_name: str | None = None

    # Result artifact export
    results_dir: str = "results"
    results_s3_prefix: str = "results"
    results_portfolio_tickers: str = "NVDA,JPM,WMT,GE,DG"
    results_local_copy_enabled: bool = True
    results_upload_to_s3: bool = True

    # LLM
    openai_api_key: str | None = None
    gemini_api_key: str | None = None

    # SEC / EDGAR
    sec_user_agent: str = "PE-OrgAIR (Northeastern) yourname@northeastern.edu"

    # RapidAPI / Glassdoor
    rapidapi_key: str | None = None
    glassdoor_rapidapi_key: str | None = None
    glassdoor_rapidapi_host: str = "glassdoor-real-time.p.rapidapi.com"
    glassdoor_company_search_path: str = "/companies/search"
    glassdoor_reviews_path: str = "/companies/reviews"
    glassdoor_reviews_page_size: int = 50
    glassdoor_cache_to_disk: bool = True
    # If true, skip multi-parameter discovery/query fallbacks to minimize API usage.
    glassdoor_disable_discovery_fallback: bool = False
    # Query param key for reviews endpoint, usually "companyId".
    glassdoor_reviews_company_id_param: str = "companyId"
    # JSON string map like {"NVDA":"40772"}.
    glassdoor_company_id_map: str | None = None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
