
from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy import String, TypeDecorator
from sqlalchemy.dialects.postgresql import JSONB
from database import Base
from datetime import datetime
import json






class JSONBString(TypeDecorator):
    impl = JSONB
    def process_bind_param(self, value, dialect):
        if isinstance(value, str):
            # преобразуем строку в dict
            return json.loads(value)
        return value

class Data(Base):
    __tablename__ = 'data'
    __table_args__ = {'schema': 'public'}

    object: Mapped[str] = mapped_column(String(50), primary_key=True)
    status: Mapped[int]
    level: Mapped[int]
    parent: Mapped[str] = mapped_column(nullable=True)
    owner: Mapped[str] = mapped_column(String(14))


class Documents(Base):
    __tablename__ = 'documents'
    __table_args__ = {'schema': 'public'}

    doc_id: Mapped[str] = mapped_column(primary_key=True)
    recieved_at: Mapped[datetime]
    document_type: Mapped[str]
    document_data: Mapped[dict] = mapped_column(JSONBString())
    processed_at: Mapped[datetime] = mapped_column(nullable=True)





























