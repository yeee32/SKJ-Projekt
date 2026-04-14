import uuid
from datetime import datetime
from pathlib import Path

import aiofiles
from fastapi import FastAPI, File, UploadFile, HTTPException, Header, Depends, Request
from fastapi.responses import FileResponse
from fastapi.routing import APIRoute
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from model import Base, FileModel, Bucket
from schemas import *

app = FastAPI(title="Object Storage Service", version="2.0.0")

# ======================
# DB SETUP
# ======================

DATABASE_URL = "sqlite:///./files.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False}, echo=True)
SessionLocal = sessionmaker(bind=engine)



def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ======================
# STORAGE
# ======================

STORAGE_ROOT = Path("storage")


def get_user_dir(user_id: str) -> Path:
    path = STORAGE_ROOT / user_id
    path.mkdir(parents=True, exist_ok=True)
    return path


# ======================
# MIDDLEWARE – API REQUEST BILLING
# ======================

@app.middleware("http")
async def count_api_requests(request: Request, call_next):
    response = await call_next(request)

    if 200 <= response.status_code < 300:
        method = request.method
        path = request.url.path
        is_write = method in ("POST", "PUT", "DELETE")

        bucket_id = None
        parts = path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "buckets":
            bucket_id = parts[1]

        if not bucket_id:
            bucket_id = request.query_params.get("bucket_id")

        if bucket_id:
            db = SessionLocal()
            try:
                bucket = db.query(Bucket).filter(Bucket.id == bucket_id).first()
                if bucket:
                    if is_write:
                        bucket.count_write_requests += 1
                    else:
                        bucket.count_read_requests += 1
                    db.commit()
            finally:
                db.close()

    return response


# ======================
# ENDPOINTS
# ======================

@app.post(
    "/files/upload",
    response_model=UploadResponse,
    status_code=201,
    summary="Upload file"
)
async def upload_file(
    file: UploadFile = File(...),
    bucket_id: str = None,
    x_user_id: str = Header(default="anonymous"),
    x_internal_source: str = Header(default=None),
    db: Session = Depends(get_db),
):
    is_internal = (x_internal_source or "").lower() == "true"
    file_id = str(uuid.uuid4())
    user_dir = get_user_dir(x_user_id)
    dest_path = user_dir / file_id

    async with aiofiles.open(dest_path, "wb") as out:
        while chunk := await file.read(65536):
            await out.write(chunk)

    size = dest_path.stat().st_size

    db_file = FileModel(
        id=file_id,
        user_id=x_user_id,
        filename=file.filename,
        path=str(dest_path),
        size=size,
        created_at=datetime.utcnow().isoformat(),
        bucket_id=bucket_id  
    )

    db.add(db_file)

    if bucket_id:
        bucket = db.query(Bucket).filter(Bucket.id == bucket_id).first()
        if bucket:
            bucket.current_storage_bytes += size
            if is_internal:
                bucket.internal_transfer_bytes += size
            else:
                bucket.ingress_bytes += size
            db.add(bucket)

    db.commit()
    db.refresh(db_file)

    return UploadResponse(
        id=db_file.id,
        filename=db_file.filename,
        size=db_file.size
    )

@app.post("/buckets", response_model=BucketResponse)
def create_bucket(
    bucket: BucketCreate,
    db: Session = Depends(get_db)
):
    existing = db.query(Bucket).filter(Bucket.name == bucket.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Bucket already exists")

    new_bucket = Bucket(
        id=str(uuid.uuid4()),
        name=bucket.name
    )

    db.add(new_bucket)
    db.commit()
    db.refresh(new_bucket)

    return new_bucket

@app.get("/buckets/{bucket_id}/objects", response_model=BucketObjectsResponse)
def list_bucket_files(
    bucket_id: str,
    db: Session = Depends(get_db)
):
    files = db.query(FileModel).filter(
        FileModel.bucket_id == bucket_id,
        FileModel.is_deleted == False
    ).all()

    return BucketObjectsResponse(
        bucket_id=bucket_id,
        files=[FileMeta.model_validate(f) for f in files]
    )

@app.get("/files", response_model=FileListResponse)
def list_files(
    x_user_id: str = Header(default="anonymous"),
    db: Session = Depends(get_db),
):
    files = db.query(FileModel).filter(
        FileModel.user_id == x_user_id,
        FileModel.is_deleted == False
    ).all()

    return FileListResponse(
        files=[FileMeta.model_validate(f) for f in files]
    )


@app.get(
    "/files/{file_id}",
    summary="Download file"
)
def download_file(
    file_id: str,
    x_user_id: str = Header(default="anonymous"),
    x_internal_source: str = Header(default=None),
    db: Session = Depends(get_db),
):
    is_internal = (x_internal_source or "").lower() == "true"
    record = db.query(FileModel).filter(FileModel.id == file_id).first()

    if not record:
        raise HTTPException(status_code=404, detail="File not found")

    if record.user_id != x_user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    file_path = Path(record.path)

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File missing from disk")

    # Billing – egress nebo internal
    if record.bucket_id:
        bucket = db.query(Bucket).filter(Bucket.id == record.bucket_id).first()
        if bucket:
            if is_internal:
                bucket.internal_transfer_bytes += record.size
            else:
                bucket.egress_bytes += record.size
            db.commit()

    return FileResponse(
        path=str(file_path),
        filename=record.filename,
        media_type="application/octet-stream",
    )


@app.delete(
    "/files/{file_id}",
    status_code=204,
    summary="Delete file"
)
def delete_file(
    file_id: str,
    x_user_id: str = Header(default="anonymous"),
    db: Session = Depends(get_db),
):
    record = db.query(FileModel).filter(FileModel.id == file_id).first()

    if not record:
        raise HTTPException(status_code=404, detail="File not found")

    if record.user_id != x_user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    if record.bucket_id:
        bucket = db.query(Bucket).filter(Bucket.id == record.bucket_id).first()
        if bucket:
            # Při soft delete storage NEKLESÁ – soubor fyzicky stále existuje
            # current_storage_bytes se sníží až při případném hard delete
            db.add(bucket)

    record.is_deleted = True
    db.commit()

@app.get("/buckets", response_model=BucketListResponse)
def list_buckets(db: Session = Depends(get_db)):
    buckets = db.query(Bucket).all()
    
    return BucketListResponse(buckets=buckets)


@app.get("/buckets/{bucket_id}/billing/", response_model=BucketBillingResponse)
def get_bucket_billing(
    bucket_id: str,
    db: Session = Depends(get_db)
):
    bucket = db.query(Bucket).filter(Bucket.id == bucket_id).first()
    
    if not bucket:
        raise HTTPException(status_code=404, detail="Bucket not found")

    return BucketBillingResponse(
        bucket_id=bucket.id,
        name=bucket.name,
        current_storage_bytes=bucket.current_storage_bytes or 0,
        ingress_bytes=bucket.ingress_bytes or 0,
        egress_bytes=bucket.egress_bytes or 0,
        internal_transfer_bytes=bucket.internal_transfer_bytes or 0,
        count_write_requests=bucket.count_write_requests or 0,
        count_read_requests=bucket.count_read_requests or 0,
    )