import os
from sqlalchemy import create_engine, MetaData, select, and_
from loguru import logger as loggy



class Databaseobject:
    def __init__(self, db_engine:str = "postgres"):
        self.host = os.getenv("DATABASE_HOST")
        self.database = os.getenv("DATABASE_NAME")
        self.user = os.getenv("DATABASE_USER")
        self.password = os.getenv("DATABASE_PASSWORD")
        self.port = os.getenv("DATABASE_PORT", 5432)
        self.db_engine = db_engine

    def build_url(self):
        if self.db_engine == "postgres":
            return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    # Create the SQLAlchemy engine
    def get_engine(self, url:str=None):
        if url is None:
            return create_engine(self.build_url())
