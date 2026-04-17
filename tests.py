"""
Spuštění:
    pytest tests.py -v
"""

import json
import pytest
import asyncio
import uuid
import websockets

BROKER_WS_URL = "ws://localhost:8000/broker"


# ======================
# POMOCNÉ FUNKCE
# ======================

async def connect_subscriber(topic: str):
    """
    Připojí se jako subscriber k danému tématu a vrátí WebSocket.
    Po subscribe přeskočí všechny historické deliver zprávy z DB,
    aby další recv() v testech dostaly pouze nové zprávy.
    """
    ws = await websockets.connect(BROKER_WS_URL)
    await ws.send(json.dumps({"action": "subscribe", "topic": topic}))
    ack = json.loads(await ws.recv())
    assert ack["status"] == "subscribed"
    assert ack["topic"] == topic

    # Přeskočíme historické zprávy (durable queue replay)
    while True:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=0.3)
            msg = json.loads(raw)
            if msg.get("action") != "deliver":
                break
        except asyncio.TimeoutError:
            break  # Žádná další historická zpráva

    return ws


async def publish_message(topic: str, payload: dict) -> dict:
    """Připojí se jako publisher, odešle zprávu a vrátí ACK od brokera."""
    async with websockets.connect(BROKER_WS_URL) as ws:
        await ws.send(json.dumps({
            "action": "publish",
            "topic": topic,
            "payload": payload,
        }))
        ack = json.loads(await ws.recv())
        return ack


async def ack_message(ws, message_id: int) -> dict:
    await ws.send(json.dumps({"action": "ack", "message_id": message_id}))
    return json.loads(await ws.recv())


# ======================
# TESTY
# ======================

@pytest.mark.asyncio
async def test_connect_and_disconnect():
    """
    Test 1: Úspěšné připojení a odpojení klienta.
    Klient se připojí, přihlásí k tématu a korektně odpojí.
    """
    ws = await websockets.connect(BROKER_WS_URL)

    # Přihlásíme se k odběru
    await ws.send(json.dumps({"action": "subscribe", "topic": "test_connect"}))
    ack = json.loads(await ws.recv())

    assert ack["status"] == "subscribed"
    assert ack["topic"] == "test_connect"

    # Korektní odpojení
    await ws.close()

    # WebSocket by měl být uzavřen bez chyby
    # Novější websockets nemá atribut 'closed', ověříme uzavření přes výjimku
    try:
        await ws.send("ping")
        pytest.fail("WebSocket by měl být uzavřen, ale stále přijímá zprávy")
    except Exception:
        pass  # Správné chování – send na uzavřený WebSocket vyhodí výjimku


@pytest.mark.asyncio
async def test_message_delivered_to_correct_topic():
    """
    Test 2: Zpráva odeslaná do tématu X dorazí klientovi,
    který odebírá téma X.
    """
    topic = "test_topic_x"
    payload = {"temperature": 42.0, "unit": "C"}

    # Připojíme subscribera
    subscriber_ws = await connect_subscriber(topic)

    try:
        # Publikujeme zprávu do stejného tématu
        ack = await publish_message(topic, payload)
        assert ack["status"] == "published"

        # Subscriber by měl dostat zprávu
        raw = await asyncio.wait_for(subscriber_ws.recv(), timeout=3.0)
        msg = json.loads(raw)

        assert msg["action"] == "deliver"
        assert msg["topic"] == topic
        assert msg["payload"] == payload
        assert "message_id" in msg

        ack = await ack_message(subscriber_ws, msg["message_id"])
        assert ack["status"] == "acknowledged"
        assert ack["message_id"] == msg["message_id"]

    finally:
        await subscriber_ws.close()


@pytest.mark.asyncio
async def test_message_not_delivered_to_wrong_topic():
    """
    Test 3: Zpráva odeslaná do tématu Y nedorazí klientovi,
    který odebírá pouze téma X.
    """
    topic_x = "test_isolation_x"
    topic_y = "test_isolation_y"
    payload = {"event": "should_not_arrive"}

    # Subscriber poslouchá pouze téma X
    subscriber_ws = await connect_subscriber(topic_x)

    try:
        # Publikujeme zprávu do tématu Y
        ack = await publish_message(topic_y, payload)
        assert ack["status"] == "published"

        # Subscriber tématu X by NEMĚL dostat zprávu z tématu Y
        try:
            raw = await asyncio.wait_for(subscriber_ws.recv(), timeout=1.0)
            msg = json.loads(raw)
            pytest.fail(f"Subscriber dostal zprávu z jiného tématu: {msg}")
        except asyncio.TimeoutError:
            pass

    finally:
        await subscriber_ws.close()