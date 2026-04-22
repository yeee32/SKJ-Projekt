"""
image_tests.py – Integrační test pro Image Processing Worker
"""

import asyncio
import json
import uuid
import pytest
import websockets

BROKER_URL  = "ws://localhost:8000/broker"
TOPIC_JOBS  = "image.jobs"
TOPIC_DONE  = "image.done"
TOTAL_JOBS  = 10
TIMEOUT_SEC = 30


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


def make_jobs(n: int = TOTAL_JOBS) -> list[dict]:
    file_id = "test-file-does-not-exist"
    ops = [
        {"operation": "invert",     "params": {}},
        {"operation": "flip",       "params": {}},
        {"operation": "grayscale",  "params": {}},
        {"operation": "brightness", "params": {"delta": 40}},
        {"operation": "brightness", "params": {"delta": -30}},
        {"operation": "crop",       "params": {"top": 10, "left": 10, "bottom": 10, "right": 10}},
        {"operation": "invert",     "params": {}},
        {"operation": "flip",       "params": {}},
        {"operation": "grayscale",  "params": {}},
        {"operation": "exploit-op", "params": {}},
    ]
    return [
        {"job_id": str(uuid.uuid4()), "file_id": file_id, "bucket_id": None,
         "user_id": "test-user", "operation": op["operation"], "params": op["params"]}
        for op in ops[:n]
    ]


async def safe_ack(ws, message_id: int):
    """ACKne zprávu BEZ čtení odpovědi – odpověď přijde do hlavní smyčky a tam se ignoruje."""
    await ws.send(json.dumps({"action": "ack", "message_id": message_id}))


async def run_integration_test():
    jobs = make_jobs(TOTAL_JOBS)
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

            # Ignorujeme "acknowledged", "published" – čekáme jen na "deliver"
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
    invalid_job = next(j for j in jobs if j["operation"] == "exploit-op")
    invalid_result = received_done.get(invalid_job["job_id"])
    assert invalid_result is not None, "Výsledek pro neplatnou operaci nebyl přijat"
    assert invalid_result["status"] == "error", (
        f"Neplatná operace měla vrátit 'error', ale vrátila: {invalid_result['status']}"
    )
    print(f"\nVšechny testy prošly!")


@pytest.mark.asyncio
async def test_worker_processes_all_jobs():
    await run_integration_test()


if __name__ == "__main__":
    asyncio.run(run_integration_test())