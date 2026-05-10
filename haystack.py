"""
haystack.py – Haystack Storage Node

Fyzicky spravuje velké Volume soubory (append-only) a poskytuje
HTTP endpoint pro čtení dat.

Komunikace:
  - Naslouchá brokeru na tématu: storage.write
  - Potvrzení odesílá do tématu:  storage.ack

Spuštění:
    uvicorn haystack:app --port 8001 --reload

Konfigurace (env proměnné):
    BROKER_WS_URL   ws://localhost:8000/broker
    HAYSTACK_DIR    ./volumes
    MAX_VOLUME_SIZE 104857600   (100 MB)
"""

import asyncio
import json
import os
import struct
from pathlib import Path

import msgpack
import websockets
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response

# ======================
# KONFIGURACE
# ======================

BROKER_WS_URL   = os.getenv("BROKER_WS_URL",   "ws://localhost:8000/broker")
HAYSTACK_DIR    = Path(os.getenv("HAYSTACK_DIR", "./volumes"))
MAX_VOLUME_SIZE = int(os.getenv("MAX_VOLUME_SIZE", 100 * 1024 * 1024))  # 100 MB

TOPIC_WRITE = "storage.write"
TOPIC_ACK   = "storage.ack"

# ======================
# STAV APLIKACE
# ======================

# Aktuálně aktivní volume – číslo a otevřený file handle
_current_volume_id: int = 1
_current_file = None          # binární file handle (mode "ab+")
_volume_lock  = asyncio.Lock()

app = FastAPI(title="Haystack Storage Node", version="1.0.0")


# ======================
# POMOCNÉ FUNKCE PRO VOLUME
# ======================

def _volume_path(volume_id: int) -> Path:
    return HAYSTACK_DIR / f"volume_{volume_id}.dat"


def _open_volume(volume_id: int):
    """Otevře (nebo vytvoří) volume soubor v append+read binárním režimu."""
    path = _volume_path(volume_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    # "r+b" pokud existuje, jinak vytvoříme přes "a+b"
    # Vždy používáme "ab+" – seek na tell() dá aktuální konec
    f = open(path, "ab+")
    return f


def _detect_current_volume() -> int:
    """Při startu najde nejvyšší existující volume číslo."""
    HAYSTACK_DIR.mkdir(parents=True, exist_ok=True)
    ids = []
    for p in HAYSTACK_DIR.glob("volume_*.dat"):
        try:
            num = int(p.stem.split("_")[1])
            ids.append(num)
        except (IndexError, ValueError):
            pass
    return max(ids) if ids else 1


# ======================
# STARTUP / SHUTDOWN
# ======================

@app.on_event("startup")
async def startup_event():
    global _current_volume_id, _current_file

    HAYSTACK_DIR.mkdir(parents=True, exist_ok=True)
    _current_volume_id = _detect_current_volume()
    _current_file = _open_volume(_current_volume_id)

    size = _volume_path(_current_volume_id).stat().st_size
    print(f"[haystack] Startuji s volume_{_current_volume_id}.dat "
          f"(aktuální velikost: {size:,} B, limit: {MAX_VOLUME_SIZE:,} B)")

    # Spustíme subscriber na pozadí – nesmí blokovat FastAPI
    asyncio.create_task(storage_write_subscriber())


@app.on_event("shutdown")
async def shutdown_event():
    global _current_file
    if _current_file and not _current_file.closed:
        _current_file.flush()
        _current_file.close()
        print("[haystack] Volume soubor uzavřen.")


# ======================
# APPEND-ONLY ZÁPIS (s rotací)
# ======================

async def append_to_volume(data: bytes) -> tuple[int, int, int]:
    """
    Zapíše data na konec aktivního volume.
    Pokud by zápis překročil MAX_VOLUME_SIZE, provede rotaci (nový volume).

    Vrátí: (volume_id, offset, size)
    """
    global _current_volume_id, _current_file

    async with _volume_lock:
        # Zjistíme aktuální velikost
        current_size = _volume_path(_current_volume_id).stat().st_size

        # Rotace: nový volume pokud bychom přesáhli limit
        if current_size + len(data) > MAX_VOLUME_SIZE and current_size > 0:
            _current_file.flush()
            _current_file.close()
            _current_volume_id += 1
            _current_file = _open_volume(_current_volume_id)
            print(f"[haystack] Rotace → volume_{_current_volume_id}.dat")

        # Seek na konec (v append módu je tell() = konec, ale pro jistotu)
        _current_file.seek(0, 2)   # SEEK_END
        offset = _current_file.tell()

        # Zápis
        _current_file.write(data)
        _current_file.flush()       # záruky persistence

        size = len(data)
        volume_id = _current_volume_id

    return volume_id, offset, size


# ======================
# STORAGE.WRITE SUBSCRIBER
# ======================

async def storage_write_subscriber():
    """
    Naslouchá brokeru na tématu storage.write.
    Pro každou příchozí zprávu:
      1. Zapíše binární payload do aktivního volume
      2. Odešle ACK zprávu do storage.ack
    Běží jako background task a při výpadku spojení se reconnektuje.
    """
    print(f"[haystack] Připojuji subscriber na {BROKER_WS_URL} → '{TOPIC_WRITE}'")

    while True:
        try:
            async with websockets.connect(BROKER_WS_URL) as ws:
                # Přihlásit se k odběru
                await ws.send(json.dumps({"action": "subscribe", "topic": TOPIC_WRITE}))
                raw = await ws.recv()
                resp = json.loads(raw)

                if resp.get("status") != "subscribed":
                    print(f"[haystack] Neočekávaná odpověď při subscribe: {resp}")
                    await asyncio.sleep(3)
                    continue

                print(f"[haystack] Subscriber aktivní – naslouchám '{TOPIC_WRITE}'")

                async for raw_msg in ws:
                    try:
                        # Broker posílá JSON (text frames)
                        if isinstance(raw_msg, bytes):
                            msg = msgpack.unpackb(raw_msg, raw=False)
                        else:
                            msg = json.loads(raw_msg)
                    except Exception as e:
                        print(f"[haystack] Nelze deserializovat zprávu: {e}")
                        continue

                    if msg.get("action") != "deliver":
                        # Může přijít ack odpověď apod. – ignorujeme
                        continue

                    message_id = msg.get("message_id")
                    payload    = msg.get("payload", {})
                    object_id  = payload.get("object_id")

                    # Binární data jsou v payloadu jako list bajtů nebo base64 string
                    raw_data = payload.get("data")
                    if raw_data is None:
                        print(f"[haystack] Zpráva bez 'data', přeskakuji (object_id={object_id})")
                        if message_id:
                            await ws.send(json.dumps({"action": "ack", "message_id": message_id}))
                        continue

                    # Dekódujeme data – mohou přijít jako list intů (msgpack) nebo bytes
                    if isinstance(raw_data, (bytes, bytearray)):
                        file_bytes = bytes(raw_data)
                    elif isinstance(raw_data, list):
                        file_bytes = bytes(raw_data)
                    else:
                        print(f"[haystack] Neznámý typ dat: {type(raw_data)}, přeskakuji")
                        if message_id:
                            await ws.send(json.dumps({"action": "ack", "message_id": message_id}))
                        continue

                    # Zápis do volume
                    try:
                        volume_id, offset, size = await append_to_volume(file_bytes)
                    except Exception as e:
                        print(f"[haystack] Chyba při zápisu: {e}")
                        if message_id:
                            await ws.send(json.dumps({"action": "ack", "message_id": message_id}))
                        continue

                    print(f"[haystack] Zapsáno object_id={object_id} → "
                          f"vol={volume_id} offset={offset} size={size}")

                    # ACK brokeru – zpráva přijata
                    if message_id:
                        await ws.send(json.dumps({"action": "ack", "message_id": message_id}))

                    # Odeslat potvrzení do storage.ack
                    ack_payload = {
                        "object_id": object_id,
                        "volume_id": volume_id,
                        "offset":    offset,
                        "size":      size,
                    }
                    await ws.send(json.dumps({
                        "action":  "publish",
                        "topic":   TOPIC_ACK,
                        "payload": ack_payload,
                    }))
                    # Odpověď na publish nemusíme číst – přijde jako další zpráva
                    # a naše smyčka ji přeskočí (action != "deliver")

        except websockets.ConnectionClosed:
            print("[haystack] Spojení s brokerem ztraceno – restartuji za 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[haystack] Chyba subscriber: {e} – restartuji za 5s...")
            await asyncio.sleep(5)


# ======================
# HTTP ENDPOINT – ČTENÍ
# ======================

@app.get(
    "/volume/{volume_id}/{offset}/{size}",
    summary="Čtení dat z volume souboru",
    response_class=Response,
)
async def read_from_volume(volume_id: int, offset: int, size: int):
    """
    Otevře volume_{volume_id}.dat, skočí na offset a vrátí přesně
    {size} bajtů jako binární odpověď.
    """
    path = _volume_path(volume_id)

    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Volume {volume_id} neexistuje")

    if offset < 0 or size <= 0:
        raise HTTPException(status_code=400, detail="Neplatný offset nebo size")

    try:
        # Čtení je rychlá synchronní operace – volume soubory jsou lokální
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read(size)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Chyba při čtení: {e}")

    if len(data) == 0:
        raise HTTPException(status_code=404, detail="Na daném offsetu nejsou žádná data")

    # Vrátíme jako binární stream (S3 Gateway pozná media type z DB)
    return Response(content=data, media_type="application/octet-stream")


# ======================
# ADMIN ENDPOINT – SEZNAM VOLUMES
# ======================

@app.get("/volumes", summary="Seznam existujících volumes a jejich velikostí")
async def list_volumes():
    result = []
    for p in sorted(HAYSTACK_DIR.glob("volume_*.dat")):
        try:
            vid = int(p.stem.split("_")[1])
            result.append({
                "volume_id": vid,
                "path":      str(p),
                "size_bytes": p.stat().st_size,
            })
        except (IndexError, ValueError):
            pass
    return {"volumes": result}


# ======================
# SPUŠTĚNÍ (přímé)
# ======================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("haystack:app", host="0.0.0.0", port=8001, reload=False)