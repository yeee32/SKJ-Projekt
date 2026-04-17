import asyncio
import json
import time
import websockets
import sys
try:
    import msgpack
    MSGPACK_AVAILABLE = True
except ImportError:
    MSGPACK_AVAILABLE = False


BROKER_URL = "ws://localhost:8000/broker"

NUM_PUBLISHERS = 5
NUM_SUBSCRIBERS = 5
MESSAGES_PER_PUBLISHER = 10000
TOPIC = "benchmark"

INFLIGHT = 1000  # max neacknutých publish zpráv


# ======================
# SERIALIZACE
# ======================

def serialize(data, fmt):
    if fmt == "json":
        return json.dumps(data)
    return msgpack.packb(data, use_bin_type=True)


def deserialize(raw, fmt):
    if fmt == "json":
        return json.loads(raw)
    return msgpack.unpackb(raw, raw=False)


# ======================
# SUBSCRIBER (S ACK)
# ======================

async def subscriber(expected_count, ready_event, fmt, should_ack=True):
    try:
        async with websockets.connect(BROKER_URL, ping_interval=None) as ws:
            await ws.send(serialize({"action": "subscribe", "topic": TOPIC}, fmt))
            
            try:
                resp_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                resp = deserialize(resp_raw, fmt)
            except asyncio.TimeoutError:
                print(f"Subscriber: TIMEOUT - žádná odpověď od serveru za 5s")
                return 0
                
            if resp.get("status") != "subscribed":
                print(f"Subscriber: neočekávaná odpověď: {resp}")
                return 0

            ready_event.set()
            print(f"Subscriber: ready")
            # ... zbytek funkce
    except Exception as e:
        print(f"Subscriber: VÝJIMKA: {type(e).__name__}: {e}")
        return 0


# ======================
# PUBLISHER (INFLIGHT WINDOW)
# ======================

async def publisher(count, fmt):
    async with websockets.connect(BROKER_URL, ping_interval=None) as ws:
        packet = serialize({
            "action": "publish",
            "topic": TOPIC,
            "payload": {"v": 1},
        }, fmt)

        sent = 0
        acked = 0

        async def sender():
            nonlocal sent, acked
            while sent < count:
                if (sent - acked) < INFLIGHT:
                    await ws.send(packet)
                    sent += 1
                else:
                    await asyncio.sleep(0)

        async def receiver():
            nonlocal acked
            while acked < count:
                msg = deserialize(await ws.recv(), fmt)
                if msg.get("status") == "published":
                    acked += 1

        await asyncio.gather(sender(), receiver())
        return sent


# ======================
# BENCHMARK
# ======================

async def run_benchmark(fmt):
    total_messages = NUM_PUBLISHERS * MESSAGES_PER_PUBLISHER

    ready_events = [asyncio.Event() for _ in range(NUM_SUBSCRIBERS)]

    subs = [
        asyncio.create_task(
            subscriber(
                total_messages,
                ready_events[i],
                fmt,
                should_ack=(i == 0),
            )
        )
        for i in range(NUM_SUBSCRIBERS)
    ]

    print(f"Čekám na {NUM_SUBSCRIBERS} subscriberů...")
    try:
        await asyncio.wait_for(
            asyncio.gather(*(e.wait() for e in ready_events)),
            timeout=10.0
        )
    except asyncio.TimeoutError:
        print("TIMEOUT: ne všichni subscribeři se připojili!")
        for i, e in enumerate(ready_events):
            print(f"  Subscriber {i}: {'ready' if e.is_set() else 'NOT READY'}")
        for s in subs:
            s.cancel()
        return 0, 0, 0

    start = time.perf_counter()

    pubs = [
        asyncio.create_task(publisher(MESSAGES_PER_PUBLISHER, fmt))
        for _ in range(NUM_PUBLISHERS)
    ]

    await asyncio.gather(*pubs)
    await asyncio.gather(*subs)

    elapsed = time.perf_counter() - start
    throughput = total_messages / elapsed
    fanout_throughput = (total_messages * NUM_SUBSCRIBERS) / elapsed

    return throughput, fanout_throughput, elapsed


# ======================
# MAIN
# ======================

async def main():
    fmt = sys.argv[1] if len(sys.argv) > 1 else "json"
    print(f"Benchmark start ({fmt})\n")
    t, fan, e = await run_benchmark(fmt)
    print(f"{fmt.upper()}: {t:,.0f} msg/s | fan-out: {fan:,.0f} msg/s | {e:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())