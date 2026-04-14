from pydantic import BaseModel, Field

class FileMeta(BaseModel):
    id: str
    user_id: str
    filename: str = Field(..., min_length=1)
    path: str
    size: int = Field(..., gt=0)
    created_at: str

    model_config = {
        "from_attributes": True
    }

class UploadResponse(BaseModel):
    id: str
    filename: str
    size: int

class FileListResponse(BaseModel):
    files: list[FileMeta]

class BucketCreate(BaseModel):
    name: str = Field(..., min_length=1)

class BucketResponse(BaseModel):
    id: str
    name: str
    created_at: str

    current_storage_bytes: int = 0
    ingress_bytes: int = 0
    egress_bytes: int = 0
    internal_transfer_bytes: int = 0
    count_write_requests: int = 0
    count_read_requests: int = 0

    model_config = {
        "from_attributes": True
    }

class BucketObjectsResponse(BaseModel):
    bucket_id: str
    files: list[FileMeta]

class BucketListResponse(BaseModel):
    buckets: list[BucketResponse]

class BucketBillingResponse(BaseModel):
    bucket_id: str
    name: str
    current_storage_bytes: int = 0
    ingress_bytes: int = 0
    egress_bytes: int = 0
    internal_transfer_bytes: int = 0
    count_write_requests: int = 0
    count_read_requests: int = 0

    model_config = {
        "from_attributes": True
    }