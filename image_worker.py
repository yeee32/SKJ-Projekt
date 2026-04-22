"""
worker.py – Image Processing Worker (Event-Driven)

Naslouchá brokeru na tématu image.jobs, stahuje obrázky z S3 Gateway,
provádí NumPy operace a výsledky nahrává zpět. Potvrzení odesílá do image.done.

Podporované operace:
  - invert      ... inverze barev (negativ)
  - flip        ... horizontální překlopení
  - crop        ... ořez (vyžaduje: top, left, bottom, right)
  - brightness  ... úprava jasu (vyžaduje: delta, celé číslo -255..255)
  - grayscale   ... převod do odstínů šedi

Spuštění:
    python worker.py
    python worker.py --broker ws://localhost:8000/broker --api http://localhost:8000
"""

import asyncio
import argparse
import json
import io
import sys

import websockets
import httpx
import numpy as np
from PIL import Image

BROKER_URL = "ws://localhost:8000/broker"
API_BASE   = "http://localhost:8000"

TOPIC_JOBS = "image.jobs"
TOPIC_DONE = "image.done"


# ──────────────────────────────────────────────
# NumPy operace
# ──────────────────────────────────────────────

def op_invert(img_array: np.ndarray) -> np.ndarray:
    """Inverze barev – vektorizovaná operace."""
    return 255 - img_array


def op_flip(img_array: np.ndarray) -> np.ndarray:
    """Horizontální překlopení (zrcadlo)."""
    return img_array[:, ::-1, :]


def op_crop(img_array: np.ndarray, params: dict) -> np.ndarray:
    """
    Ořez obrázku.
    Parametry: top, left, bottom, right (všechny v pixelech, nezáporné).
    Vyvolá ValueError pokud by ořez přesáhl rozměry obrázku.
    """
    h, w = img_array.shape[:2]
    top    = int(params.get("top",    0))
    left   = int(params.get("left",   0))
    bottom = int(params.get("bottom", 0))
    right  = int(params.get("right",  0))

    if top < 0 or left < 0 or bottom < 0 or right < 0:
        raise ValueError("Parametry crop musí být nezáporné.")
    if top + bottom >= h:
        raise ValueError(f"Ořez (top={top}, bottom={bottom}) přesahuje výšku obrázku ({h}px).")
    if left + right >= w:
        raise ValueError(f"Ořez (left={left}, right={right}) přesahuje šířku obrázku ({w}px).")

    row_end = h - bottom if bottom > 0 else h
    col_end = w - right  if right  > 0 else w
    return img_array[top:row_end, left:col_end, :]


def op_brightness(img_array: np.ndarray, params: dict) -> np.ndarray:
    """
    Úprava jasu se saturací (žádné přetečení uint8).
    Parametr: delta (integer, –255 .. 255).
    """
    delta = int(params.get("delta", 50))
    tmp = img_array.astype(np.int16) + delta
    return np.clip(tmp, 0, 255).astype(np.uint8)


def op_grayscale(img_array: np.ndarray) -> np.ndarray:
    """
    Převod do odstínů šedi – vážený průměr dle citlivosti lidského oka.
    Výsledek je 3kanálový obrázek (H×W×3), aby šel uložit jako RGB JPEG.
    """
    gray = (
        0.299 * img_array[:, :, 0].astype(np.float32)
        + 0.587 * img_array[:, :, 1].astype(np.float32)
        + 0.114 * img_array[:, :, 2].astype(np.float32)
    ).astype(np.uint8)
    # Vrátíme jako 3D pole, aby Pillow uložilo standardní RGB
    return np.stack([gray, gray, gray], axis=-1)


OPERATIONS = {
    "invert":     lambda arr, p: op_invert(arr),
    "flip":       lambda arr, p: op_flip(arr),
    "crop":       op_crop,
    "brightness": op_brightness,
    "grayscale":  lambda arr, p: op_grayscale(arr),
}


# ──────────────────────────────────────────────
# HTTP helpers (stahování / nahrávání)
# ──────────────────────────────────────────────

async def download_image(http: httpx.AsyncClient, file_id: str, user_id: str) -> np.ndarray:
    """Stáhne soubor z S3 Gateway a vrátí NumPy pole."""
    r = await http.get(
        f"{API_BASE}/files/{file_id}",
        headers={"x-user-id": user_id, "x-internal-source": "true"},
    )
    r.raise_for_status()
    img = Image.open(io.BytesIO(r.content)).convert("RGB")
    return np.array(img)


async def upload_image(
    http: httpx.AsyncClient,
    img_array: np.ndarray,
    filename: str,
    bucket_id: str | None,
    user_id: str,
) -> str:
    """Zakóduje NumPy pole jako JPEG a nahraje na S3 Gateway. Vrátí nové file_id."""
    buf = io.BytesIO()
    Image.fromarray(img_array).save(buf, format="JPEG", quality=92)
    buf.seek(0)

    params = {}
    if bucket_id:
        params["bucket_id"] = bucket_id

    r = await http.post(
        f"{API_BASE}/files/upload",
        params=params,
        headers={"x-user-id": user_id, "x-internal-source": "true"},
        files={"file": (filename, buf, "image/jpeg")},
    )
    r.raise_for_status()
    return r.json()["id"]


# ──────────────────────────────────────────────
# Zpracování jedné úlohy
# ──────────────────────────────────────────────

async def process_job(job: dict, http: httpx.AsyncClient) -> dict:
    """
    Zpracuje jednu image job.
    Vrátí slovník, který bude odeslán do tématu image.done.
    """
    file_id   = job["file_id"]
    operation = job["operation"]
    params    = job.get("params", {})
    user_id   = job.get("user_id", "anonymous")
    bucket_id = job.get("bucket_id")
    job_id    = job.get("job_id", file_id)

    # Neznámá operace
    if operation not in OPERATIONS:
        return {
            "job_id":    job_id,
            "status":    "error",
            "error":     f"Neznámá operace: '{operation}'. Povolené operace: {list(OPERATIONS)}",
            "file_id":   file_id,
            "operation": operation,
        }

    try:
        # 1. Stáhnout obrázek
        img_array = await download_image(http, file_id, user_id)

        # 2. Provést operaci
        result_array = OPERATIONS[operation](img_array, params)

        # 3. Sestavit název výstupního souboru
        out_filename = f"{file_id}_{operation}.jpg"

        # 4. Nahrát výsledek
        new_file_id = await upload_image(http, result_array, out_filename, bucket_id, user_id)

        return {
            "job_id":      job_id,
            "status":      "done",
            "file_id":     file_id,
            "new_file_id": new_file_id,
            "operation":   operation,
        }

    except Exception as exc:
        return {
            "job_id":    job_id,
            "status":    "error",
            "error":     str(exc),
            "file_id":   file_id,
            "operation": operation,
        }


# ──────────────────────────────────────────────
# WebSocket smyčka
# ──────────────────────────────────────────────

async def run_worker():
    print(f"[worker] Připojuji se k brokeru: {BROKER_URL}")
    print(f"[worker] S3 Gateway: {API_BASE}")
    print(f"[worker] Naslouchám tématu: {TOPIC_JOBS}\n")

    async with httpx.AsyncClient(timeout=60.0) as http:
        while True:
            try:
                async with websockets.connect(BROKER_URL) as ws:
                    # Přihlásit se k odběru
                    await ws.send(json.dumps({"action": "subscribe", "topic": TOPIC_JOBS}))
                    raw_ack = await ws.recv()
                    ack = json.loads(raw_ack)
                    if ack.get("status") == "subscribed":
                        print(f"[worker] Přihlášen k odběru '{TOPIC_JOBS}'")
                    else:
                        print(f"[worker] Neočekávaná odpověď při subscribe: {ack}")
                        await asyncio.sleep(3)
                        continue

                    try:
                        while True:
                            raw = await ws.recv()
                            try:
                                msg = json.loads(raw)
                            except Exception:
                                print(f"[worker] Nelze parsovat zprávu: {raw!r}")
                                continue

                            if msg.get("action") != "deliver":
                                continue

                            message_id = msg.get("message_id")
                            payload    = msg.get("payload", {})

                            print(f"[worker] Zpráva #{message_id} – operace: {payload.get('operation')}")

                            # Zpracovat úlohu
                            result = await process_job(payload, http)
                            print(f"[worker] Výsledek #{message_id}: {result['status']}"
                                  + (f" – {result.get('error')}" if result.get("error") else ""))

                            # ACK brokeru – potvrdíme přijetí zprávy
                            if message_id:
                                await ws.send(json.dumps({"action": "ack", "message_id": message_id}))

                            # Publikovat výsledek do image.done
                            # Pozor: NEČTEME odpověď na publish zde – broker může mezitím
                            # poslat další deliver zprávu a ta by se spolkla a zahodila.
                            # Odpovědi (ack na ack, published) přijdou do hlavní smyčky
                            # a budou ignorovány podmínkou `if msg.get("action") != "deliver"`.
                            await ws.send(json.dumps({
                                "action":  "publish",
                                "topic":   TOPIC_DONE,
                                "payload": result,
                            }))

                    except websockets.ConnectionClosed:
                        print("[worker] Spojení uzavřeno, restartuji...")

            except Exception as exc:
                print(f"[worker] Chyba připojení: {exc} – zkouším znovu za 5s")
                await asyncio.sleep(5)


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Image Processing Worker")
    p.add_argument("--broker", default=BROKER_URL, help="URL WebSocket brokera")
    p.add_argument("--api",    default=API_BASE,   help="Základní URL S3 Gateway")
    return p


def main():
    args = build_parser().parse_args()
    global BROKER_URL, API_BASE
    BROKER_URL = args.broker
    API_BASE   = args.api

    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        print("\n[worker] Ukončen.")


if __name__ == "__main__":
    main()