import uuid
from sqlalchemy import Column, Text, func, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Bronze(Base):
    __tablename__ = "bronze_layer"

    id = Column(UUID, primary_key=True, default=uuid.uuid4)
    transaction_id = Column(Text, nullable=False)
    payload = Column(JSONB, nullable=False)
    kafka_metadata = Column(JSONB, nullable=False)
    airflow_run_id = Column(Text, nullable=True)
    processed_status = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<Bronze(id={self.id}, transaction_id={self.transaction_id})>"


