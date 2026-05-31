import os

class Settings:
    POSTGRES_DSN = os.getenv("POSTGRES_DSN", "http://localhost:5432")


settings = Settings()
