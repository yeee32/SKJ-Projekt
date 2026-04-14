from sqlalchemy import Boolean, ForeignKey, Column, String, Integer
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime


Base = declarative_base()

class FileModel(Base):
    __tablename__ = "files"

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, index=True)
    filename = Column(String)
    path = Column(String)
    size = Column(Integer)
    created_at = Column(String)

    bucket_id = Column(String, ForeignKey("buckets.id"))
    bucket = relationship("Bucket", back_populates="files")

    is_deleted = Column(Boolean, default=False)

class Bucket(Base):
    __tablename__ = "buckets"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    created_at = Column(String, default=lambda: datetime.utcnow().isoformat())

    current_storage_bytes = Column(Integer, default=0)
    ingress_bytes = Column(Integer, default=0)
    egress_bytes = Column(Integer, default=0)
    internal_transfer_bytes = Column(Integer, default=0)

    count_write_requests = Column(Integer, default=0)
    count_read_requests = Column(Integer, default=0)

    files = relationship("FileModel", back_populates="bucket")


