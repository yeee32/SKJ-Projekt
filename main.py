"""
main.py – S3 Gateway (Haystack edition)

Změny oproti původní verzi:
  - POST /files/upload   → odešle soubor do storage.write (broker), vrátí HTTP 202
  - Background task      → naslouchá storage.ack, aktualizuje DB (volume_id, offset, size, status)
  - GET  /files/{id}     → interně volá Haystack Node přes httpx, přeposílá data uživateli
  - DELETE /files/{id}   → soft delete (beze změny)
  - Model FileModel rozšířen o: status, volume_id, offset, volume_size

Spuštění:
    uvicorn main:app --port 8000 --reload

Env proměnné:
    BROKER_WS_URL   ws://localhost:8000/broker
    HAYSTACK_URL    http://localhost:8001
"""

import uuid
import json
import asyncio
import os
import itertools
from datetime import datetime
from pathlib import Path
from typing import Any

import msgpack
import httpx
import websockets
from fastapi import FastAPI, File, UploadFile, HTTPException, Header, Depends, Request, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, event, func, Column, String, Integer, Boolean
from sqlalchemy.orm import sessionmaker, Session
from model import Base, FileModel, Bucket, QueuedMessage
from schemas import *

# ======================
# KONFIGURACE
# ======================

BROKER_WS_URL = os.getenv("BROKER_WS_URL", "ws://localhost:8000/broker")
HAYSTACK_URL  = os.getenv("HAYSTACK_URL",  "http://localhost:8001")

TOPIC_WRITE = "storage.write"
TOPIC_ACK   = "storage.ack"

app = FastAPI(title="S3 Gateway (Haystack)", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================
# DB SETUP
# ======================

DATABASE_URL = "sqlite:///./files.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False}, echo=False)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)

message_queue = asyncio.Queue()
ack_queue     = asyncio.Queue()
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
# DB – MIGRACE (přidání nových sloupců pokud chybí)
# ======================

def _ensure_haystack_columns():
    """Přidá sloupce pro Haystack pokud ještě neexistují (SQLite ALTER TABLE)."""
    db = SessionLocal()
    try:
        from sqlalchemy import text, inspect
        insp = inspect(engine)
        cols = {c["name"] for c in insp.get_columns("files")}
        with engine.connect() as conn:
            if "status" not in cols:
                conn.execute(text("ALTER TABLE files ADD COLUMN status TEXT DEFAULT 'legacy'"))
            if "volume_id" not in cols:
                conn.execute(text("ALTER TABLE files ADD COLUMN volume_id INTEGER"))
            if "offset" not in cols:
                conn.execute(text('ALTER TABLE files ADD COLUMN "offset" INTEGER'))
            if "volume_size" not in cols:
                conn.execute(text("ALTER TABLE files ADD COLUMN volume_size INTEGER"))
            conn.commit()
    except Exception as e:
        print(f"[gateway] Migrace sloupců: {e}")
    finally:
        db.close()


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
        q = asyncio.Queue()
        self.queues[websocket] = q
        self.tasks[websocket] = asyncio.create_task(self._writer_task(websocket, q))

    async def _writer_task(self, ws: WebSocket, q: asyncio.Queue):
        try:
            while True:
                data = await q.get()
                try:
                    if isinstance(data, bytes):
                        await ws.send_bytes(data)
                    else:
                        await ws.send_text(data)
                    await asyncio.sleep(0)
                except:
                    break
                finally:
                    q.task_done()
        except asyncio.CancelledError:
            pass

    def disconnect_all(self, websocket: WebSocket):
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
        j_data = m_data = None
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


# ======================
# DB WORKERS (broker persistence)
# ======================

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
        db.query(QueuedMessage).filter(QueuedMessage.id.in_(ids)).update(
            {"is_delivered": True}, synchronize_session=False
        )
        db.commit()
    finally: db.close()

async def message_db_worker():
    while True:
        try:
            m = await message_queue.get()
            batch = [m]
            while len(batch) < 1000:
                try: batch.append(message_queue.get_nowait())
                except asyncio.QueueEmpty: break
            await run_in_threadpool(db_store_messages_batch, batch)
            for _ in range(len(batch)): message_queue.task_done()
        except Exception as e:
            print(f"[gateway] message_db_worker error: {e}")
            await asyncio.sleep(0.1)

async def ack_db_worker():
    while True:
        try:
            a = await ack_queue.get()
            batch = [a]
            while len(batch) < 1000:
                try: batch.append(ack_queue.get_nowait())
                except asyncio.QueueEmpty: break
            await run_in_threadpool(db_ack_messages_batch, batch)
            for _ in range(len(batch)): ack_queue.task_done()
        except Exception as e:
            print(f"[gateway] ack_db_worker error: {e}")
            await asyncio.sleep(0.1)

def db_load_undelivered(topic: str):
    db = SessionLocal()
    try: return db.query(QueuedMessage).filter(
        QueuedMessage.topic == topic,
        QueuedMessage.is_delivered == False
    ).all()
    finally: db.close()

def get_max_id_from_db():
    db = SessionLocal()
    try:
        res = db.query(func.max(QueuedMessage.id)).scalar()
        return int(res) if res else 0
    except: return 0
    finally: db.close()


# ======================
# STORAGE.ACK LISTENER (background task)
# ======================

async def storage_ack_listener():
    """
    Naslouchá brokeru na tématu storage.ack.
    Jakmile dorazí potvrzení od Haystack Node, aktualizuje FileModel
    v DB: volume_id, offset, volume_size, status = "ready".
    Billing se účtuje až zde (Eventual Consistency).
    """
    print(f"[gateway] Spouštím storage.ack listener → {BROKER_WS_URL}")

    while True:
        try:
            async with websockets.connect(BROKER_WS_URL) as ws:
                await ws.send(json.dumps({"action": "subscribe", "topic": TOPIC_ACK}))
                raw = await ws.recv()
                resp = json.loads(raw)
                if resp.get("status") != "subscribed":
                    print(f"[gateway] Neočekávaná odpověď storage.ack subscribe: {resp}")
                    await asyncio.sleep(3)
                    continue

                print(f"[gateway] storage.ack listener aktivní")

                async for raw_msg in ws:
                    print(f"[gateway][ack-listener] Přijata zpráva: {str(raw_msg)[:120]}")
                    try:
                        msg = json.loads(raw_msg) if isinstance(raw_msg, str) else msgpack.unpackb(raw_msg, raw=False)
                    except Exception:
                        continue

                    if msg.get("action") != "deliver":
                        continue

                    message_id = msg.get("message_id")
                    payload    = msg.get("payload", {})

                    object_id  = payload.get("object_id")
                    volume_id  = payload.get("volume_id")
                    offset     = payload.get("offset")
                    size       = payload.get("size")

                    if not all(v is not None for v in [object_id, volume_id, offset, size]):
                        print(f"[gateway] Neúplný ACK payload: {payload}")
                        if message_id:
                            await ws.send(json.dumps({"action": "ack", "message_id": message_id}))
                        continue

                    print(f"[gateway] Zpracovávám ACK: object_id={object_id} "
                          f"vol={volume_id} offset={offset} size={size}")

                    # Aktualizace DB – hodnoty předáme jako argumenty, ne closure!
                    # (closure v cyklu by zachytila referenci, ne hodnotu)
                    def _update_db(oid=object_id, vid=volume_id, off=offset, sz=size):
                        db = SessionLocal()
                        try:
                            record = db.query(FileModel).filter(FileModel.id == oid).first()
                            if not record:
                                print(f"[gateway] ACK pro neznámý object_id: {oid}")
                                return

                            record.volume_id   = vid
                            record.offset      = off
                            record.volume_size = sz
                            record.status      = "ready"

                            # Billing – účtujeme až teď (Eventual Consistency)
                            if record.bucket_id:
                                bucket = db.query(Bucket).filter(Bucket.id == record.bucket_id).first()
                                if bucket:
                                    bucket.current_storage_bytes += sz
                                    bucket.ingress_bytes         += sz
                                    db.add(bucket)

                            db.commit()
                            print(f"[gateway] ✓ status=ready: object_id={oid} "
                                  f"vol={vid} offset={off} size={sz}")
                        finally:
                            db.close()

                    await run_in_threadpool(_update_db)

                    # Potvrdíme brokeru
                    if message_id:
                        await ws.send(json.dumps({"action": "ack", "message_id": message_id}))

        except websockets.ConnectionClosed:
            print("[gateway] storage.ack listener: spojení ztraceno – restartuji za 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[gateway] storage.ack listener chyba: {e} – restartuji za 5s...")
            await asyncio.sleep(5)


# ======================
# STARTUP
# ======================

@app.on_event("startup")
async def startup_event():
    global message_id_counter
    Base.metadata.create_all(bind=engine)
    _ensure_haystack_columns()

    start_id = await run_in_threadpool(get_max_id_from_db)
    message_id_counter = itertools.count(start_id + 1)

    asyncio.create_task(message_db_worker())
    asyncio.create_task(ack_db_worker())
    asyncio.create_task(storage_ack_listener())   # ← nový task


# ======================
# MIDDLEWARE – BILLING (počítání requestů)
# ======================

@app.middleware("http")
async def count_api_requests(request: Request, call_next):
    response = await call_next(request)

    if 200 <= response.status_code < 300:
        method   = request.method
        path     = request.url.path
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
                    if is_write: bucket.count_write_requests += 1
                    else:        bucket.count_read_requests  += 1
                    db.commit()
            finally:
                db.close()

    return response


# ======================
# WEBSOCKET BROKER ENDPOINT
# ======================

async def send_undelivered(websocket: WebSocket, topic: str):
    try:
        undelivered = await run_in_threadpool(db_load_undelivered, topic)
        for row in undelivered:
            try:
                p_h = json.loads(row.payload.decode("utf-8"))
                manager.send_to(websocket, {
                    "action": "deliver", "topic": row.topic,
                    "message_id": row.id, "payload": p_h,
                })
            except: continue
    except Exception as e:
        print(f"[gateway] send_undelivered error: {e}")

@app.websocket("/broker")
async def broker_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
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
                    recipients = await manager.broadcast(
                        {"action": "deliver", "topic": t, "message_id": mid, "payload": p},
                        t, sender=websocket
                    )
                    manager.send_to(websocket, {
                        "status": "published", "topic": t,
                        "message_id": mid, "recipients": recipients
                    })

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
# SCHEMAS (lokální)
# ======================

class ProcessRequest(BaseModel):
    operation: str
    params: dict[str, Any] = {}


# ======================
# UPLOAD – ASYNCHRONNÍ (Haystack)
# ======================

@app.post(
    "/files/upload",
    status_code=202,
    summary="Upload souboru (asynchronní – Haystack)"
)
async def upload_file(
    file: UploadFile = File(...),
    bucket_id: str = None,
    x_user_id: str = Header(default="anonymous"),
    x_internal_source: str = Header(default=None),
    db: Session = Depends(get_db),
):
    """
    Přijme soubor, uloží metadata do DB se statusem 'uploading'
    a odešle binární data přes broker do Haystack Node.
    Vrátí HTTP 202 Accepted – soubor ještě není fyzicky na disku.
    Status se změní na 'ready' až po přijetí storage.ack.
    """
    file_id = str(uuid.uuid4())

    # Přečteme celý soubor do paměti (nutné pro odeslání přes broker)
    file_bytes = await file.read()
    size       = len(file_bytes)

    if size == 0:
        raise HTTPException(status_code=400, detail="Soubor je prázdný")

    # Uložíme metadata do DB se statusem "uploading"
    db_file = FileModel(
        id         = file_id,
        user_id    = x_user_id,
        filename   = file.filename,
        path       = "",              # fyzická cesta neznámá – řídí Haystack
        size       = size,
        created_at = datetime.utcnow().isoformat(),
        bucket_id  = bucket_id,
        status     = "uploading",
    )
    db.add(db_file)
    db.commit()

    # Odeslání přes broker do storage.write
    # Binární data serializujeme jako list bajtů (JSON-safe)
    write_payload = {
        "object_id": file_id,
        "filename":  file.filename,
        "data":      list(file_bytes),   # list[int], 0-255
    }

    mid = next(message_id_counter)
    message_queue.put_nowait((TOPIC_WRITE, write_payload, mid))
    await manager.broadcast(
        {"action": "deliver", "topic": TOPIC_WRITE, "message_id": mid, "payload": write_payload},
        TOPIC_WRITE,
    )

    return {
        "id":       file_id,
        "filename": file.filename,
        "size":     size,
        "status":   "uploading",
        "message":  "Soubor byl přijat a čeká na fyzické uložení (Haystack).",
    }


# ======================
# DOWNLOAD – PŘES HAYSTACK
# ======================

@app.get("/files/{file_id}", summary="Stažení souboru přes Haystack Node")
async def download_file(
    file_id: str,
    x_user_id: str = Header(default="anonymous"),
    x_internal_source: str = Header(default=None),
    db: Session = Depends(get_db),
):
    is_internal = (x_internal_source or "").lower() == "true"
    record = db.query(FileModel).filter(FileModel.id == file_id).first()

    if not record:
        raise HTTPException(status_code=404, detail="Soubor nenalezen")

    if record.is_deleted:
        raise HTTPException(status_code=410, detail="Soubor byl smazán")

    if record.user_id != x_user_id and not is_internal:
        raise HTTPException(status_code=403, detail="Přístup odepřen")

    status = getattr(record, "status", "legacy")

    # Legacy soubory (před Haystack migrací) – vrátíme z disku
    if status == "legacy":
        from fastapi.responses import FileResponse
        file_path = Path(record.path)
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Soubor chybí na disku")
        if record.bucket_id:
            bucket = db.query(Bucket).filter(Bucket.id == record.bucket_id).first()
            if bucket:
                if is_internal: bucket.internal_transfer_bytes += record.size
                else:           bucket.egress_bytes            += record.size
                db.commit()
        return FileResponse(path=str(file_path), filename=record.filename,
                            media_type="application/octet-stream")

    if status == "uploading":
        raise HTTPException(status_code=202, detail="Soubor se teprve nahrává, zkuste za chvíli")

    if status != "ready":
        raise HTTPException(status_code=503, detail=f"Soubor není dostupný (status: {status})")

    volume_id   = record.volume_id
    offset      = record.offset
    volume_size = record.volume_size

    if any(v is None for v in [volume_id, offset, volume_size]):
        raise HTTPException(status_code=500, detail="Chybí Haystack metadata")

    # Interní volání na Haystack Node
    haystack_url = f"{HAYSTACK_URL}/volume/{volume_id}/{offset}/{volume_size}"

    async def _stream():
        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream("GET", haystack_url) as resp:
                if resp.status_code != 200:
                    raise HTTPException(status_code=502, detail="Haystack Node nedostupný")
                async for chunk in resp.aiter_bytes(65536):
                    yield chunk

    # Billing egress
    if record.bucket_id:
        bucket = db.query(Bucket).filter(Bucket.id == record.bucket_id).first()
        if bucket:
            if is_internal: bucket.internal_transfer_bytes += record.size
            else:           bucket.egress_bytes            += record.size
            db.commit()

    return StreamingResponse(
        _stream(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{record.filename}"'},
    )


# ======================
# DELETE – SOFT DELETE
# ======================

@app.delete("/files/{file_id}", status_code=204, summary="Soft delete souboru")
def delete_file(
    file_id: str,
    x_user_id: str = Header(default="anonymous"),
    db: Session = Depends(get_db),
):
    """
    Pouze nastaví is_deleted = True v DB.
    Haystack Node se o mazání vůbec nedozví – fyzická data ve volume
    zůstávají až do kompakce.
    """
    record = db.query(FileModel).filter(FileModel.id == file_id).first()

    if not record:
        raise HTTPException(status_code=404, detail="Soubor nenalezen")
    if record.user_id != x_user_id:
        raise HTTPException(status_code=403, detail="Přístup odepřen")
    if record.is_deleted:
        return  # Idempotentní – již smazáno

    record.is_deleted = True

    # Aktualizace billing (uvolněné místo)
    if record.bucket_id:
        bucket = db.query(Bucket).filter(Bucket.id == record.bucket_id).first()
        if bucket and bucket.current_storage_bytes:
            bucket.current_storage_bytes = max(
                0, bucket.current_storage_bytes - (record.volume_size or record.size or 0)
            )
            db.add(bucket)

    db.commit()


# ======================
# SEZNAM SOUBORŮ UŽIVATELE
# ======================

@app.get("/files", response_model=FileListResponse)
def list_files(
    x_user_id: str = Header(default="anonymous"),
    include_uploading: bool = False,
    db: Session = Depends(get_db),
):
    q = db.query(FileModel).filter(
        FileModel.user_id == x_user_id,
        FileModel.is_deleted == False,
    )
    if not include_uploading:
        # Vrátíme jen soubory které jsou fyzicky dostupné ke stažení
        q = q.filter(FileModel.status.in_(["ready", "legacy"]))
    files = q.all()
    return FileListResponse(files=[FileMeta.model_validate(f) for f in files])


# ======================
# BUCKET ENDPOINTY
# ======================

@app.post("/buckets", response_model=BucketResponse)
def create_bucket(bucket: BucketCreate, db: Session = Depends(get_db)):
    existing = db.query(Bucket).filter(Bucket.name == bucket.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Bucket already exists")
    new_bucket = Bucket(id=str(uuid.uuid4()), name=bucket.name)
    db.add(new_bucket)
    db.commit()
    db.refresh(new_bucket)
    return new_bucket


@app.get("/buckets", response_model=BucketListResponse)
def list_buckets(db: Session = Depends(get_db)):
    return BucketListResponse(buckets=db.query(Bucket).all())


@app.get("/buckets/{bucket_id}/objects", response_model=BucketObjectsResponse)
def list_bucket_files(bucket_id: str, db: Session = Depends(get_db)):
    files = db.query(FileModel).filter(
        FileModel.bucket_id == bucket_id,
        FileModel.is_deleted == False
    ).all()
    return BucketObjectsResponse(
        bucket_id=bucket_id,
        files=[FileMeta.model_validate(f) for f in files]
    )


@app.get("/buckets/{bucket_id}/billing/", response_model=BucketBillingResponse)
def get_bucket_billing(bucket_id: str, db: Session = Depends(get_db)):
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


# ======================
# IMAGE PROCESSING ENDPOINT
# ======================

@app.post("/buckets/{bucket_id}/objects/{file_id}/process",
          summary="Spustit image processing úlohu (asynchronní)")
async def process_image(
    bucket_id: str,
    file_id: str,
    body: ProcessRequest,
    x_user_id: str = Header(default="anonymous"),
    db: Session = Depends(get_db),
):
    record = db.query(FileModel).filter(FileModel.id == file_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Soubor nenalezen")
    if record.user_id != x_user_id:
        raise HTTPException(status_code=403, detail="Přístup odepřen")

    job_id = str(uuid.uuid4())
    job_payload = {
        "job_id": job_id, "file_id": file_id,
        "bucket_id": bucket_id, "user_id": x_user_id,
        "operation": body.operation, "params": body.params,
    }
    mid = next(message_id_counter)
    message_queue.put_nowait(("image.jobs", job_payload, mid))
    await manager.broadcast(
        {"action": "deliver", "topic": "image.jobs", "message_id": mid, "payload": job_payload},
        "image.jobs",
    )
    return {"status": "processing_started", "job_id": job_id, "message_id": mid}


# ======================
# ADMIN – SEZNAM OBJEKTŮ PRO KOMPAKCI
# ======================

@app.get("/admin/volumes/{volume_id}/objects",
         summary="Seznam aktivních objektů v daném volume (pro kompakci)")
def list_volume_objects(volume_id: int, db: Session = Depends(get_db)):
    """
    Vrátí seznam nesmazaných souborů v daném volume.
    Používá compact.py při kompakci.
    """
    records = db.query(FileModel).filter(
        FileModel.volume_id == volume_id,
        FileModel.is_deleted == False,
        FileModel.status == "ready",
    ).all()
    return {
        "volume_id": volume_id,
        "objects": [
            {
                "object_id": r.id,
                "offset":    r.offset,
                "size":      r.volume_size,
                "filename":  r.filename,
            }
            for r in records
        ]
    }


@app.patch("/admin/objects/{object_id}/location",
           summary="Aktualizace Haystack lokace objektu (pro kompakci)")
def update_object_location(
    object_id: str,
    body: dict,
    db: Session = Depends(get_db),
):
    """
    Aktualizuje volume_id a offset objektu po kompakci.
    Volá compact.py.
    """
    record = db.query(FileModel).filter(FileModel.id == object_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Objekt nenalezen")

    if "volume_id" in body: record.volume_id = body["volume_id"]
    if "offset"    in body: record.offset    = body["offset"]
    if "volume_id" in body or "offset" in body:
        db.commit()

    return {"status": "updated", "object_id": object_id}


# ======================
# SPUŠTĚNÍ
# ======================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)