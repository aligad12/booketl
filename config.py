from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

# Build the absolute path to .env so it works on Windows
ENV_PATH = Path(__file__).parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=str(ENV_PATH), extra="ignore")

    snowflake_account:   str
    snowflake_user:      str
    snowflake_password:  str
    snowflake_database:  str = "BOOKETL_DB"
    snowflake_warehouse: str = "BOOKETL_WH"
    snowflake_role:      str = "ACCOUNTADMIN"

    scrape_base_url:  str = "https://books.toscrape.com"
    scrape_max_pages: int = 50

    @property
    def snowflake_conn(self) -> dict:
        return {
            "account":   self.snowflake_account,
            "user":      self.snowflake_user,
            "password":  self.snowflake_password,
            "database":  self.snowflake_database,
            "warehouse": self.snowflake_warehouse,
            "role":      self.snowflake_role,
        }


settings = Settings()