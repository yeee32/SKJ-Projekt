"""
haystack.py – Haystack Storage Node

Spuštění:
    uvicorn haystack:app --port 8001

Env proměnné:
    BROKER_URI      ws://127.0.0.1:8000/broker
    VOLUME_DIR      ./volumes
    MAX_VOLUME_SIZE 104857600  (100 MB)
    GATEWAY_URL     http://127.0.0.1:8000
"""

import os
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import msgpack
import websockets
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response

# ======================
# KONFIGURACE
# ======================

BROKER_URI      = os.getenv("BROKER_URI",       "ws://127.0.0.1:8000/broker")
VOLUME_DIR      = os.getenv("VOLUME_DIR",        "volumes")
MAX_VOLUME_SIZE = int(os.getenv("MAX_VOLUME_SIZE", 100 * 1024 * 1024))
GATEWAY_URL     = os.getenv("GATEWAY_URL",       "http://127.0.0.1:8000")

os.makedirs(VOLUME_DIR, exist_ok=True)


# ======================
# VOLUME MANAGER
# ======================

class VolumeManager:
    def __init__(self):
        self.current_volume_id = 1
        self.file_handle = None
        self._init_volume()

    def _init_volume(self):
        files = [f for f in os.listdir(VOLUME_DIR)
                 if f.startswith("volume_") and f.endswith(".dat")]
        if files:
            ids = [int(f.split("_")[1].split(".")[0]) for f in files]
            self.current_volume_id = max(ids)
        else:
            self.current_volume_id = 1
        self._open_current()
        size = os.path.getsize(self._current_path())
        print(f"[haystack] Startuji s volume_{self.current_volume_id}.dat "
              f"(velikost: {size:,} B, limit: {MAX_VOLUME_SIZE:,} B)")

    def _current_path(self):
        return os.path.join(VOLUME_DIR, f"volume_{self.current_volume_id}.dat")

    def _open_current(self):
        if self.file_handle and not self.file_handle.closed:
            self.file_handle.close()
        self.file_handle = open(self._current_path(), "ab+")

    def write_data(self, data: bytes) -> tuple[int, int, int]:
        self.file_handle.seek(0, os.SEEK_END)
        current_size = self.file_handle.tell()

        if current_size + len(data) > MAX_VOLUME_SIZE:
            self.current_volume_id += 1
            self._open_current()
            print(f"[haystack] Rotace → volume_{self.current_volume_id}.dat")
            self.file_handle.seek(0, os.SEEK_END)

        offset = self.file_handle.tell()
        self.file_handle.write(data)
        self.file_handle.flush()
        return self.current_volume_id, offset, len(data)

    def read_data(self, volume_id: int, offset: int, size: int) -> bytes | None:
        path = os.path.join(VOLUME_DIR, f"volume_{volume_id}.dat")
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            f.seek(offset)
            return f.read(size)


volume_manager = VolumeManager()


# ======================
# BROKER SUBSCRIBER
# ======================

async def broker_subscriber():
    """
    Naslouchá storage.write přes msgpack WebSocket.
    Binární data stahuje z Gateway přes HTTP /internal/pending/{object_id}.
    """
    while True:
        try:
            async with websockets.connect(
                BROKER_URI,
                max_size=None,
                ping_interval=None,
            ) as ws:
                # Přihlásit se jako msgpack klient
                await ws.send(msgpack.packb({"action": "subscribe", "topic": "storage.write"}))
                raw_ack = await ws.recv()
                ack = msgpack.unpackb(raw_ack, raw=False)
                if ack.get("status") != "subscribed":
                    print(f"[haystack] Neočekávaná odpověď subscribe: {ack}")
                    await asyncio.sleep(3)
                    continue

                print(f"[haystack] Subscriber aktivní – naslouchám 'storage.write'")

                # Přeskočit historické zprávy
                print(f"[haystack] Přeskakuji historické zprávy...")
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                        hist = msgpack.unpackb(raw, raw=False)
                        if hist.get("action") == "deliver":
                            mid = hist.get("message_id")
                            if mid:
                                await ws.send(msgpack.packb({"action": "ack", "message_id": mid}))
                    except asyncio.TimeoutError:
                        break
                    except Exception:
                        break
                print(f"[haystack] Čekám na nové zprávy.")

                while True:
                    try:
                        raw_msg = await ws.recv()
                    except websockets.ConnectionClosed:
                        print("[haystack] Spojení uzavřeno – restartuji...")
                        break

                    try:
                        decoded = msgpack.unpackb(raw_msg, raw=False)
                    except Exception as e:
                        print(f"[haystack] Nelze dekódovat zprávu: {e}")
                        continue

                    if decoded.get("action") != "deliver":
                        continue
                    if decoded.get("topic") != "storage.write":
                        continue

                    payload   = decoded.get("payload", {})
                    msg_id    = decoded.get("message_id")
                    object_id = payload.get("object_id")

                    # Stáhneme data z Gateway přes HTTP
                    gateway_url = payload.get("gateway_url")
                    if not gateway_url:
                        print(f"[haystack] Zpráva bez gateway_url, přeskakuji (object_id={object_id})")
                        if msg_id:
                            await ws.send(msgpack.packb({"action": "ack", "message_id": msg_id}))
                        continue

                    try:
                        async with httpx.AsyncClient(timeout=60.0) as http:
                            resp = await http.get(gateway_url)
                            if resp.status_code == 404:
                                print(f"[haystack] Pending upload nenalezen: {object_id}")
                                if msg_id:
                                    await ws.send(msgpack.packb({"action": "ack", "message_id": msg_id}))
                                continue
                            resp.raise_for_status()
                            file_bytes = resp.content
                    except Exception as e:
                        print(f"[haystack] Chyba stahování z Gateway: {e}")
                        if msg_id:
                            await ws.send(msgpack.packb({"action": "ack", "message_id": msg_id}))
                        continue

                    # Zápis do volume
                    try:
                        vol_id, offset, size = volume_manager.write_data(file_bytes)
                    except Exception as e:
                        print(f"[haystack] Chyba zápisu: {e}")
                        if msg_id:
                            await ws.send(msgpack.packb({"action": "ack", "message_id": msg_id}))
                        continue

                    print(f"[haystack] Zapsáno object_id={object_id} → "
                          f"vol={vol_id} offset={offset} size={size}")

                    # ACK brokeru
                    if msg_id:
                        await ws.send(msgpack.packb({"action": "ack", "message_id": msg_id}))

                    # Publish storage.ack
                    await ws.send(msgpack.packb({
                        "action":  "publish",
                        "topic":   "storage.ack",
                        "payload": {
                            "object_id": object_id,
                            "volume_id": vol_id,
                            "offset":    offset,
                            "size":      size,
                        },
                    }))

        except websockets.ConnectionClosed:
            print("[haystack] Spojení ztraceno – restartuji za 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[haystack] Chyba: {e} – restartuji za 5s...")
            await asyncio.sleep(5)


# ======================
# LIFESPAN
# ======================

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(broker_subscriber())
    yield
    task.cancel()


app = FastAPI(title="Haystack Storage Node", version="2.0.0", lifespan=lifespan)


# ======================
# HTTP ENDPOINT – ČTENÍ
# ======================

@app.get("/volume/{volume_id}/{offset}/{size}")
async def read_from_volume(volume_id: int, offset: int, size: int):
    data = volume_manager.read_data(volume_id, offset, size)
    if data is None:
        raise HTTPException(status_code=404, detail="Volume nebo soubor nenalezen")
    return Response(content=data, media_type="application/octet-stream")


# ======================
# ADMIN – SEZNAM VOLUMES
# ======================

@app.get("/volumes")
async def list_volumes():
    result = []
    for f in sorted(Path(VOLUME_DIR).glob("volume_*.dat")):
        try:
            vid = int(f.stem.split("_")[1])
            result.append({"volume_id": vid, "path": str(f), "size_bytes": f.stat().st_size})
        except (IndexError, ValueError):
            pass
    return {"volumes": result}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("haystack:app", host="0.0.0.0", port=8001, reload=False)