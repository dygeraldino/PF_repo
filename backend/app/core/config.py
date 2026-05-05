from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "PaaS Deployments API"
    DATABASE_URL: str
    JWT_SECRET: str = "your-super-secret-jwt-key"

    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

settings = Settings()
