from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "PaaS Deployments API"
    DATABASE_URL: str
    JWT_SECRET: str = "your-super-secret-jwt-key"
    SUPABASE_URL: str = ""
    SUPABASE_ANON_KEY: str = ""
    SUPABASE_SERVICE_ROLE_KEY: str = ""
    INGRESS_BASE_DOMAIN: str = "13.86.117.95.sslip.io"
    # RabbitMQ — usa el servicio de docker-compose en local, o una URL externa
    RABBITMQ_URL: str = "amqp://guest:guest@rabbitmq:5672/"
    # Kubernetes — True = simulación para demo, False = clúster real
    SIMULATE_K8S: bool = True
    K8S_SERVER_OVERRIDE: str = ""

    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

settings = Settings()
