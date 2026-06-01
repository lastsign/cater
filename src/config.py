import os

class Settings:
    POSTGRES_DSN = os.getenv(
        "POSTGRES_DSN",
        "postgresql://{user}:{password}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "babufrik"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "babufrik"),
        ),
    )


settings = Settings()
