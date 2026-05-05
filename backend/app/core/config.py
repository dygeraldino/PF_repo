from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "PaaS Deployments API"
    DATABASE_URL: str
    JWT_SECRET: str = "your-super-secret-jwt-key"
    # RabbitMQ — usa el servicio de docker-compose en local, o una URL externa
    RABBITMQ_URL: str = "amqp://guest:guest@rabbitmq:5672/"
    # Kubernetes — True = simulación para demo, False = clúster real
    SIMULATE_K8S: bool = True

    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

settings = Settings()
