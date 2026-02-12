from .credentials import DatabaseCredentials
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session


class DBManager:

    def __init__(self, credentials: DatabaseCredentials):
        self.creds: DatabaseCredentials = credentials
        self.db_url = self._create_db_url()
        self.engine: Engine | None = None
        self.session: sessionmaker[Session] | None = None

    def _create_db_url(self):
        url = f"{self.creds.user}:{self.creds.password}@{self.creds.host}:{self.creds.port}/{self.creds.database}"
        return f"postgresql://{url}"

    def _create_engine(self) -> None:
        self.engine = create_engine(url=self.db_url)

    def _create_session(self) -> None:
        if not self.engine:
            return

        self.session = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )

    def start(self) -> dict:
        try:
            self._create_engine()
            self._create_session()
            return {"status": "ok"}

        except Exception as e:
            return {"error": str(e)}