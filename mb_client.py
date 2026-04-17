"""
mb_client.py – Message Broker klient (Pub/Sub)

Použití:
    Subscriber (JSON):
        python mb_client.py subscribe sensors

    Subscriber (MessagePack):
        python mb_client.py subscribe sensors --format msgpack

    Publisher (JSON):
        python mb_client.py publish sensors '{"temp": 22.5}'

    Publisher (MessagePack):
        python mb_client.py publish sensors '{"temp": 22.5}' --format msgpack
"""

import asyncio
import json
import sys
import argparse

import websockets

# MessagePack je volitelná závislost – upozorníme uživatele, jen pokud ho potřebuje
try:
    import msgpack
    MSGPACK_AVAILABLE = True
except ImportError:
    MSGPACK_AVAILABLE = False

BROKER_URL = "ws://localhost:8000/broker"


# ======================
# SERIALIZACE / DESERIALIZACE
# ======================

def serialize(data: dict, fmt: str) -> bytes | str:
    """Převede slovník na zprávu ve zvoleném formátu."""
    if fmt == "json":
        return json.dumps(data)
    elif fmt == "msgpack":
        if not MSGPACK_AVAILABLE:
            print("Chyba: knihovna 'msgpack' není nainstalována. Spusť: pip install msgpack")
            sys.exit(1)
        return msgpack.packb(data, use_bin_type=True)
    else:
        raise ValueError(f"Neznámý formát: {fmt}")


def deserialize(raw: bytes | str, fmt: str) -> dict:
    """Převede přijatou zprávu zpět na slovník."""
    if fmt == "json":
        return json.loads(raw)
    elif fmt == "msgpack":
        if not MSGPACK_AVAILABLE:
            print("Chyba: knihovna 'msgpack' není nainstalována. Spusť: pip install msgpack")
            sys.exit(1)
        if isinstance(raw, str):
            raw = raw.encode()
        return msgpack.unpackb(raw, raw=False)
    else:
        raise ValueError(f"Neznámý formát: {fmt}")


# ======================
# SUBSCRIBER
# ======================

async def run_subscriber(topic: str, fmt: str):
    print(f"Připojuji se k brokeru ({BROKER_URL})...")
    print(f"Formát: {fmt.upper()}")

    extra_kwargs = {}
    if fmt == "msgpack":
        extra_kwargs["subprotocols"] = []

    async with websockets.connect(BROKER_URL) as ws:
        subscribe_msg = serialize({"action": "subscribe", "topic": topic}, fmt)
        await ws.send(subscribe_msg)

        raw_ack = await ws.recv()
        ack = deserialize(raw_ack, fmt)
        if ack.get("status") == "subscribed":
            print(f"Přihlášen k odběru tématu '{topic}'")
        else:
            print(f"Neočekávaná odpověď: {ack}")
            return

        print(f"Čekám na zprávy... (ukonči pomocí Ctrl+C)\n")

        try:
            while True:
                raw = await ws.recv()

                try:

                    if fmt == "json":
                        if not isinstance(raw, str):
                            raise ValueError("Očekáván JSON (text), ale přišla binární data")
                        msg = json.loads(raw)
                    
                    elif fmt == "msgpack":
                        if not isinstance(raw, (bytes, bytearray)):
                            raise ValueError("Očekáván MessagePack (bytes), ale přišel text")
                        msg = msgpack.unpackb(raw, raw=False)
                    payload = msg.get("payload", msg)
                    recv_topic = msg.get("topic", topic)
                    print(f"[{recv_topic}] {json.dumps(payload, ensure_ascii=False)}")

                    if msg.get("action") == "deliver" and "message_id" in msg:
                        ack_msg = {
                            "action": "ack",
                            "message_id": msg["message_id"],
                        }
                        await ws.send(serialize(ack_msg, fmt))
                except Exception as e:
                    print(f"Chyba při dekódování zprávy: {e}")

        except websockets.ConnectionClosed:
            print("\nSpojení uzavřeno brokerem.")
        except asyncio.CancelledError:
            pass


# ======================
# PUBLISHER
# ======================

async def run_publisher(topic: str, payload_str: str, fmt: str):
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        print(f"Chyba: payload není platný JSON: {payload_str!r}")
        sys.exit(1)

    print(f"Připojuji se k brokeru ({BROKER_URL})...")
    print(f"Formát: {fmt.upper()}")

    async with websockets.connect(BROKER_URL) as ws:
        # Sestavíme zprávu pro broker
        message = {
            "action": "publish",
            "topic": topic,
            "payload": payload,
        }

        data = serialize(message, fmt)
        await ws.send(data)

        print(f"Publikuji do tématu '{topic}': {json.dumps(payload, ensure_ascii=False)}")

        raw_ack = await ws.recv()
        try:
            ack = deserialize(raw_ack, fmt)
            recipients = ack.get("recipients", 0)
            status = ack.get("status", "?")
            if status == "published":
                print(f"Zpráva doručena. Počet příjemců: {recipients}")
            else:
                print(f"Odpověď brokera: {ack}")
        except Exception:
            print(f"Neočekávaná odpověď: {raw_ack!r}")


# ======================
# INTERAKTIVNÍ PUBLISHER
# ======================

async def run_interactive_publisher(topic: str, fmt: str):
    print(f"Připojuji se k brokeru ({BROKER_URL})...")
    print(f"Formát: {fmt.upper()}")
    print(f"Interaktivní publisher pro téma '{topic}'")
    print(f"Zadej JSON payload a stiskni Enter. Ukonči pomocí Ctrl+C nebo 'quit'.\n")

    async with websockets.connect(BROKER_URL) as ws:
        try:
            while True:
                try:
                    payload_str = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: input(">> ")
                    )
                except EOFError:
                    break

                if payload_str.strip().lower() in ("quit", "exit", "q"):
                    print("Ukončuji.")
                    break

                try:
                    payload = json.loads(payload_str)
                except json.JSONDecodeError:
                    print(f"Neplatný JSON: {payload_str!r}")
                    continue

                message = {
                    "action": "publish",
                    "topic": topic,
                    "payload": payload,
                }

                data = serialize(message, fmt)
                await ws.send(data)

                raw_ack = await ws.recv()
                try:
                    ack = deserialize(raw_ack, fmt)
                    recipients = ack.get("recipients", 0)
                    print(f"Odesláno. Příjemců: {recipients}")
                except Exception:
                    print(f"Odpověď: {raw_ack!r}")

        except websockets.ConnectionClosed:
            print("\nSpojení uzavřeno brokerem.")
        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            print("\nUkončuji.")


# ======================
# CLI – ARGUMENT PARSER
# ======================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Message Broker klient – Publisher / Subscriber",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Příklady:
  python mb_client.py subscribe sensors
  python mb_client.py subscribe sensors --format msgpack
  python mb_client.py publish sensors '{"temp": 22.5}'
  python mb_client.py publish sensors '{"temp": 22.5}' --format msgpack
  python mb_client.py publish sensors --interactive
        """
    )

    parser.add_argument(
        "mode",
        choices=["subscribe", "publish"],
        help="Režim: 'subscribe' (příjem) nebo 'publish' (odesílání)"
    )
    parser.add_argument(
        "topic",
        help="Název tématu (např. sensors, orders, logs)"
    )
    parser.add_argument(
        "payload",
        nargs="?",
        default=None,
        help="JSON payload pro publish režim (např. '{\"temp\": 22.5}')"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["json", "msgpack"],
        default="json",
        dest="fmt",
        help="Formát serializace zpráv (výchozí: json)"
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Interaktivní režim pro publisher – posílej více zpráv"
    )
    parser.add_argument(
        "--url",
        default=BROKER_URL,
        help=f"URL brokera (výchozí: {BROKER_URL})"
    )

    return parser


# ======================
# MAIN
# ======================

def main():
    parser = build_parser()
    args = parser.parse_args()

    # Přepíšeme globální URL pokud uživatel zadal vlastní
    global BROKER_URL
    BROKER_URL = args.url

    if args.mode == "subscribe":
        try:
            asyncio.run(run_subscriber(args.topic, args.fmt))
        except KeyboardInterrupt:
            print("\nSubscriber ukončen.")

    elif args.mode == "publish":
        if args.interactive:
            try:
                asyncio.run(run_interactive_publisher(args.topic, args.fmt))
            except KeyboardInterrupt:
                print("\nPublisher ukončen.")
        else:
            if not args.payload:
                parser.error("Pro publish režim zadej payload (nebo použij --interactive)")
            asyncio.run(run_publisher(args.topic, args.payload, args.fmt))


if __name__ == "__main__":
    main()