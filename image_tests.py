"""
image_tests.py – Integrační test pro Image Processing Worker

Test simuluje odesílání 10 úloh do brokera a ověřuje,
že worker zpracuje všechny a odešle 10 potvrzovacích zpráv do image.done.

Požadavky:
  - Běžící broker (ws://localhost:8000/broker)
  - Běžící worker (python worker.py)

Spuštění:
    pytest image_tests.py -v
  nebo přímo:
    python image_tests.py
"""

import asyncio
import base64
import io
import json
import uuid
import pytest
import httpx
import websockets

BROKER_URL  = "ws://localhost:8000/broker"
API_BASE    = "http://localhost:8000"
TOPIC_JOBS  = "image.jobs"
TOPIC_DONE  = "image.done"
TOTAL_JOBS  = 10
TIMEOUT_SEC = 30
TEST_USER   = "test-user"

# Malý 50×50 testovací PNG (4 barevné čtverce) zakódovaný jako base64.
# Nevyžaduje žádné externí soubory – test je plně samostatný.
TEST_IMAGE_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAADIAAAAyCAIAAACRXR/mAAAAYElEQVR4nO3XsQ3AMAwEMSv7"
    "7+zMcIAKF+QAj4M6zT1rZm/rW1taJauQVcgqZBWyClmFrEJWIauQVcgqHs2ac9aezntna+"
    "rRa8kqZBWyClmFrEJWIauQVcgqZBWyikezfuv0BWCSfew+AAAAAElFTkSuQmCC"
)


# ──────────────────────────────────────────────
# Setup – nahrání testovacího obrázku
# ──────────────────────────────────────────────

async def upload_test_image() -> str:
    """Nahraje testovací PNG na server a vrátí file_id."""
    image_bytes = base64.b64decode(TEST_IMAGE_B64)
    async with httpx.AsyncClient() as http:
        r = await http.post(
            f"{API_BASE}/files/upload",
            headers={"x-user-id": TEST_USER},
            files={"file": ("test_image.png", io.BytesIO(image_bytes), "image/png")},
        )
        r.raise_for_status()
        file_id = r.json()["id"]
        print(f"[test] Testovací obrázek nahrán – file_id: {file_id[:8]}…")
        return file_id


# ──────────────────────────────────────────────
# Pomocné funkce
# ──────────────────────────────────────────────

async def subscribe(ws, topic: str):
    await ws.send(json.dumps({"action": "subscribe", "topic": topic}))
    raw = await ws.recv()
    ack = json.loads(raw)
    assert ack.get("status") == "subscribed", f"Neočekávaná odpověď: {ack}"


async def publish(ws, topic: str, payload: dict) -> int:
    await ws.send(json.dumps({"action": "publish", "topic": topic, "payload": payload}))
    raw = await ws.recv()
    ack = json.loads(raw)
    assert ack.get("status") == "published", f"Publish selhal: {ack}"
    return ack.get("message_id", -1)


def make_jobs(file_id: str, n: int = TOTAL_JOBS) -> list[dict]:
    """Vytvoří seznam testovacích úloh se skutečným file_id."""
    ops = [
        {"operation": "invert",     "params": {}},
        {"operation": "flip",       "params": {}},
        {"operation": "grayscale",  "params": {}},
        {"operation": "brightness", "params": {"delta": 40}},
        {"operation": "brightness", "params": {"delta": -30}},
        {"operation": "crop",       "params": {"top": 5, "left": 5, "bottom": 5, "right": 5}},
        {"operation": "invert",     "params": {}},
        {"operation": "flip",       "params": {}},
        {"operation": "grayscale",  "params": {}},
        {"operation": "exploit-op", "params": {}},  # neplatná operace – musí vrátit error, ne pád
    ]
    return [
        {
            "job_id":    str(uuid.uuid4()),
            "file_id":   file_id,
            "bucket_id": None,
            "user_id":   TEST_USER,
            "operation": op["operation"],
            "params":    op["params"],
        }
        for op in ops[:n]
    ]


async def safe_ack(ws, message_id: int):
    """ACKne zprávu BEZ čtení odpovědi – odpověď přijde do hlavní smyčky a tam se ignoruje."""
    await ws.send(json.dumps({"action": "ack", "message_id": message_id}))


# ──────────────────────────────────────────────
# Jádro testu
# ──────────────────────────────────────────────

async def run_integration_test():
    # Nejdřív nahrajeme testovací obrázek
    file_id = await upload_test_image()

    jobs = make_jobs(file_id, TOTAL_JOBS)
    sent_job_ids = {j["job_id"] for j in jobs}
    received_done: dict[str, dict] = {}

    async with websockets.connect(BROKER_URL) as publisher_ws, \
               websockets.connect(BROKER_URL) as listener_ws:

        await subscribe(listener_ws, TOPIC_DONE)
        print(f"[test] Přihlášen k odběru '{TOPIC_DONE}'")

        # Vyčistit staré nedoručené zprávy z DB (výsledky předchozích běhů)
        await asyncio.sleep(0.5)
        while True:
            try:
                raw = await asyncio.wait_for(listener_ws.recv(), timeout=0.4)
                msg = json.loads(raw)
                if msg.get("action") == "deliver":
                    if mid_ := msg.get("message_id"):
                        await safe_ack(listener_ws, mid_)
            except asyncio.TimeoutError:
                break
        print(f"[test] Stará fronta vyčištěna, odesílám joby...")

        for job in jobs:
            mid = await publish(publisher_ws, TOPIC_JOBS, job)
            print(f"[test] Odesláno job_id={job['job_id'][:8]}… mid={mid} op={job['operation']}")

        print(f"\n[test] Odesláno {TOTAL_JOBS} úloh, čekám na výsledky (timeout {TIMEOUT_SEC}s)…\n")

        deadline = asyncio.get_event_loop().time() + TIMEOUT_SEC
        while len(received_done) < TOTAL_JOBS:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break

            try:
                raw = await asyncio.wait_for(listener_ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break

            try:
                msg = json.loads(raw)
            except Exception:
                continue

            if msg.get("action") != "deliver":
                continue

            if mid_ := msg.get("message_id"):
                await safe_ack(listener_ws, mid_)

            payload = msg.get("payload", {})
            job_id = payload.get("job_id")
            if job_id and job_id in sent_job_ids:
                received_done[job_id] = payload
                status = payload.get("status", "?")
                print(f"[test] Přijato job_id={job_id[:8]}… status={status}"
                      + (f" error={payload.get('error')}" if status == "error" else
                         f" new_file_id={payload.get('new_file_id', '—')}"))

    # ──────────────────────────────────────────
    # Aserce
    # ──────────────────────────────────────────
    missing = sent_job_ids - set(received_done.keys())

    print(f"\n{'─'*50}")
    print(f"Odesláno úloh:  {TOTAL_JOBS}")
    print(f"Přijato výsledků: {len(received_done)}")
    if missing:
        print(f"Chybějící job_id: {missing}")

    assert len(received_done) == TOTAL_JOBS, (
        f"Očekáváno {TOTAL_JOBS} výsledků, přijato {len(received_done)}. Chybějící: {missing}"
    )
    for job_id, result in received_done.items():
        assert result.get("status") in ("done", "error"), (
            f"job_id={job_id} má neočekávaný status: {result.get('status')}"
        )

    # Úspěšné operace musí mít status "done"
    successful_ops = [j for j in jobs if j["operation"] != "exploit-op"]
    for job in successful_ops:
        result = received_done[job["job_id"]]
        assert result["status"] == "done", (
            f"Operace '{job['operation']}' měla vrátit 'done', ale vrátila: "
            f"{result['status']} – {result.get('error')}"
        )

    # Neplatná operace musí skončit jako error
    invalid_job = next(j for j in jobs if j["operation"] == "exploit-op")
    invalid_result = received_done[invalid_job["job_id"]]
    assert invalid_result["status"] == "error", (
        f"Neplatná operace měla vrátit 'error', ale vrátila: {invalid_result['status']}"
    )

    print(f"\nVšechny testy prošly!")


# ──────────────────────────────────────────────
# pytest entry-point
# ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_worker_processes_all_jobs():
    await run_integration_test()


# ──────────────────────────────────────────────
# Přímé spuštění (bez pytest)
# ──────────────────────────────────────────────

if __name__ == "__main__":
    asyncio.run(run_integration_test())