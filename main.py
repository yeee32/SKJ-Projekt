import uuid
import json
import asyncio
from datetime import datetime
from pathlib import Path

import msgpack
import itertools
import aiofiles
from fastapi import FastAPI, File, UploadFile, HTTPException, Header, Depends, Request, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, event, func
from sqlalchemy.orm import sessionmaker, Session
from model import Base, FileModel, Bucket, QueuedMessage
from schemas import *

app = FastAPI(title="Object Storage Service", version="2.0.0")

# ======================
# DB SETUP
# ======================
DATABASE_URL = "sqlite:///./files.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False}, echo=False)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)

message_queue = asyncio.Queue()
ack_queue = asyncio.Queue()
message_id_counter = itertools.count(1)

@event.listens_for(engine, "connect")
def set_sqlite_pragmas(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    cursor.close()

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

# ======================
# MESSAGE BROKER – CONNECTION MANAGER
# ======================
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, set[WebSocket]] = {}
        self.socket_format: dict[WebSocket, str] = {}
        self.queues: dict[WebSocket, asyncio.Queue] = {}
        self.tasks: dict[WebSocket, asyncio.Task] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.socket_format[websocket] = "json"
        # Fronta pro odchozí zprávy (neomezená pro benchmark)
        q = asyncio.Queue()
        self.queues[websocket] = q
        # Dedikovaný task pro odesílání na tento socket
        self.tasks[websocket] = asyncio.create_task(self._writer_task(websocket, q))

    async def _writer_task(self, ws: WebSocket, q: asyncio.Queue):
        """Writer task zabraňuje AssertionError a WinError 64."""
        try:
            while True:
                data = await q.get()
                try:
                    if isinstance(data, bytes):
                        await ws.send_bytes(data)
                    else:
                        await ws.send_text(data)
                    # ✅ Uvolníme loop pro Windows
                    await asyncio.sleep(0)
                except:
                    break
                finally:
                    q.task_done()
        except asyncio.CancelledError:
            pass
        finally:
            # Cleanup socketu proběhne v disconnect_all
            pass

    def disconnect_all(self, websocket: WebSocket):
        # Zrušení tasku a vymazání fronty
        t = self.tasks.pop(websocket, None)
        if t: t.cancel()
        self.queues.pop(websocket, None)
        self.socket_format.pop(websocket, None)
        for topic in list(self.active_connections.keys()):
            if websocket in self.active_connections[topic]:
                self.active_connections[topic].discard(websocket)

    def subscribe(self, websocket: WebSocket, topic: str):
        if topic not in self.active_connections:
            self.active_connections[topic] = set()
        self.active_connections[topic].add(websocket)

    def send_to(self, websocket: WebSocket, payload: dict):
        """Okamžitě zařadí zprávu do fronty (neblokuje)."""
        q = self.queues.get(websocket)
        if not q: return
        fmt = self.socket_format.get(websocket, "json")
        try:
            data = msgpack.packb(payload, use_bin_type=True) if fmt == "msgpack" else json.dumps(payload)
            q.put_nowait(data)
        except: pass

    async def broadcast(self, payload: dict, topic: str, sender=None):
        subscribers = self.active_connections.get(topic, set())
        if not subscribers: return 0

        j_data = m_data = None  # lazy

        count = 0
        for ws in list(subscribers):
            if ws is sender: continue
            q = self.queues.get(ws)
            if not q: continue
            fmt = self.socket_format.get(ws, "json")
            if fmt == "msgpack":
                if m_data is None:
                    m_data = msgpack.packb(payload, use_bin_type=True)
                q.put_nowait(m_data)
            else:
                if j_data is None:
                    j_data = json.dumps(payload)
                q.put_nowait(j_data)
            count += 1
        return count

manager = ConnectionManager()

# ✅ DB FUNKCE
def db_store_messages_batch(messages):
    db = SessionLocal()
    try:
        objs = [QueuedMessage(id=mid, topic=t, payload=json.dumps(p).encode("utf-8")) for t, p, mid in messages]
        db.add_all(objs)
        db.commit()
    finally: db.close()

def db_ack_messages_batch(ids):
    db = SessionLocal()
    try:
        db.query(QueuedMessage).filter(QueuedMessage.id.in_(ids)).update({"is_delivered": True}, synchronize_session=False)
        db.commit()
    finally: db.close()

async def message_db_worker():
    while True:
        try:
            m = await message_queue.get()
            batch = [m]
            while len(batch) < 1000:
                try:
                    batch.append(message_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await run_in_threadpool(db_store_messages_batch, batch)
            for _ in range(len(batch)):
                message_queue.task_done()
        except Exception as e:
            print(f"message_db_worker error: {e}")
            await asyncio.sleep(0.1)

async def ack_db_worker():
    while True:
        try:
            a = await ack_queue.get()
            batch = [a]
            while len(batch) < 1000:
                try:
                    batch.append(ack_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await run_in_threadpool(db_ack_messages_batch, batch)
            for _ in range(len(batch)):
                ack_queue.task_done()
        except Exception as e:
            print(f"ack_db_worker error: {e}")
            await asyncio.sleep(0.1)

def db_load_undelivered(topic: str):
    db = SessionLocal()
    try: return db.query(QueuedMessage).filter(QueuedMessage.topic == topic, QueuedMessage.is_delivered == False).all()
    finally: db.close()

def get_max_id_from_db():
    db = SessionLocal()
    try:
        res = db.query(func.max(QueuedMessage.id)).scalar()
        return int(res) if res else 0
    except: return 0
    finally: db.close()

@app.on_event("startup")
async def startup_event():
    global message_id_counter
    start_id = await run_in_threadpool(get_max_id_from_db)
    message_id_counter = itertools.count(start_id + 1)
    asyncio.create_task(message_db_worker())
    asyncio.create_task(ack_db_worker())

# ======================
# WEBSOCKET ENDPOINT – BROKER
# ======================

async def send_undelivered(websocket: WebSocket, topic: str):
    """Načte undelivered zprávy z DB a pošle je subscriberovi na pozadí."""
    try:
        undelivered = await run_in_threadpool(db_load_undelivered, topic)
        for row in undelivered:
            try:
                p_h = json.loads(row.payload.decode("utf-8"))
                manager.send_to(websocket, {
                    "action": "deliver",
                    "topic": row.topic,
                    "message_id": row.id,
                    "payload": p_h,
                })
            except:
                continue
    except Exception as e:
        print(f"send_undelivered error: {e}")

@app.websocket("/broker")
async def broker_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Maximální priorita pro čtení (aby nedocházelo k ping timeout)
            msg = await websocket.receive()
            
            if msg.get("type") == "websocket.disconnect": break
            
            try:
                if "text" in msg:
                    manager.socket_format[websocket] = "json"
                    data = json.loads(msg["text"])
                elif "bytes" in msg:
                    manager.socket_format[websocket] = "msgpack"
                    data = msgpack.unpackb(msg["bytes"], raw=False)
                else: continue
            except: continue

            action = data.get("action")

            if action == "publish":
                t, p = data.get("topic"), data.get("payload")
                if t:
                    mid = next(message_id_counter)
                    message_queue.put_nowait((t, p, mid))
                    recipients = await manager.broadcast({"action":"deliver","topic":t,"message_id":mid,"payload":p}, t, sender=websocket)
                    manager.send_to(websocket, {"status":"published","topic":t,"message_id":mid,"recipients":recipients})

            elif action == "subscribe":
                topic = data.get("topic")
                if topic:
                    manager.subscribe(websocket, topic)
                    manager.send_to(websocket, {"status": "subscribed", "topic": topic})

                    asyncio.create_task(send_undelivered(websocket, topic))


            elif action == "ack":
                mid = data.get("message_id")
                if mid:
                    await ack_queue.put(mid)
                    manager.send_to(websocket, {"status": "acknowledged", "message_id": mid})

    except WebSocketDisconnect: pass
    except: pass
    finally:
        manager.disconnect_all(websocket)
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