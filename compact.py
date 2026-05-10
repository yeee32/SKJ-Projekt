"""
compact.py – Haystack Volume Compaction (Defragmentace)

Algoritmus:
  1. Ze S3 Gateway načte seznam aktivních (nesmazaných) objektů v daném volume.
  2. Vytvoří nový soubor volume_X_compacted.dat.
  3. Postupně čte aktivní data ze starého volume a přepisuje je těsně za sebe
     do nového souboru (odstraní "díry" po smazaných souborech).
  4. Pro každý přesunutý objekt aktualizuje S3 Gateway (nový offset).
  5. Po dokončení přejmenuje compacted soubor na originální název
     a smaže starý volume.

Spuštění:
    python compact.py --volume 1
    python compact.py --volume 1 --gateway http://localhost:8000 --haystack http://localhost:8001
    python compact.py --volume 1 --dry-run    # jen zobrazí co by se stalo

Poznámka:
  Během kompakce jsou nové zápisy do volume stále možné – Haystack Node
  pokračuje v práci s dalšími volumes. Kompaktovaný volume by v produkci
  měl být nejprve označen jako "read-only" (zde zjednodušeno).
"""

import argparse
import sys
import os
import shutil
import httpx
from pathlib import Path


# ======================
# KONFIGURACE
# ======================

GATEWAY_URL  = os.getenv("GATEWAY_URL",  "http://localhost:8000")
HAYSTACK_URL = os.getenv("HAYSTACK_URL", "http://localhost:8001")
VOLUMES_DIR  = Path(os.getenv("HAYSTACK_DIR", "./volumes"))

CHUNK_SIZE = 64 * 1024  # 64 KB – velikost čtecích chunků


# ======================
# HTTP HELPERS
# ======================

def get_active_objects(volume_id: int) -> list[dict]:
    """Načte ze S3 Gateway seznam aktivních objektů v daném volume."""
    url = f"{GATEWAY_URL}/admin/volumes/{volume_id}/objects"
    resp = httpx.get(url, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()
    return data.get("objects", [])


def update_object_location(object_id: str, new_volume_id: int, new_offset: int):
    """Aktualizuje v S3 Gateway novou lokaci objektu po kompakci."""
    url  = f"{GATEWAY_URL}/admin/objects/{object_id}/location"
    body = {"volume_id": new_volume_id, "offset": new_offset}
    resp = httpx.patch(url, json=body, timeout=10.0)
    resp.raise_for_status()


def read_from_haystack(volume_id: int, offset: int, size: int) -> bytes:
    """Stáhne data objektu přímo z Haystack Node."""
    url  = f"{HAYSTACK_URL}/volume/{volume_id}/{offset}/{size}"
    resp = httpx.get(url, timeout=60.0)
    resp.raise_for_status()
    return resp.content


# ======================
# KOMPAKCE
# ======================

def compact_volume(volume_id: int, dry_run: bool = False, verbose: bool = True):
    """
    Provede kompakci jednoho volume souboru.

    Args:
        volume_id: číslo volume ke kompakci
        dry_run:   pokud True, pouze zobrazí co by se stalo, nic nezmění
        verbose:   podrobný výstup
    """
    def log(msg: str):
        if verbose:
            print(msg)

    log(f"\n{'='*60}")
    log(f"  Kompakce volume_{volume_id}.dat")
    if dry_run:
        log("  [DRY RUN – žádné změny nebudou provedeny]")
    log(f"{'='*60}\n")

    # 1. Načtení aktivních objektů ze S3 Gateway
    log(f"[1/5] Načítám aktivní objekty z Gateway ({GATEWAY_URL})...")
    try:
        objects = get_active_objects(volume_id)
    except httpx.HTTPError as e:
        print(f"CHYBA: Nelze načíst objekty z Gateway: {e}", file=sys.stderr)
        sys.exit(1)

    if not objects:
        log(f"      Volume {volume_id} neobsahuje žádné aktivní objekty – kompakce přeskočena.")
        return

    # Seřadíme objekty podle offsetu (pro sekvenční čtení disku)
    objects.sort(key=lambda o: o["offset"])

    log(f"      Nalezeno {len(objects)} aktivních objektů.")

    # Výpočet potenciální úspory (zjistíme aktuální velikost volume)
    volume_path = VOLUMES_DIR / f"volume_{volume_id}.dat"
    if not volume_path.exists():
        print(f"CHYBA: Volume soubor neexistuje: {volume_path}", file=sys.stderr)
        sys.exit(1)

    original_size = volume_path.stat().st_size
    active_size   = sum(o["size"] for o in objects)
    saved_bytes   = original_size - active_size
    saved_pct     = (saved_bytes / original_size * 100) if original_size > 0 else 0

    log(f"      Původní velikost:  {original_size:>12,} B ({original_size/1024/1024:.1f} MB)")
    log(f"      Aktivní data:      {active_size:>12,} B ({active_size/1024/1024:.1f} MB)")
    log(f"      Úspora po kompakci:{saved_bytes:>12,} B ({saved_pct:.1f} %)\n")

    if saved_bytes <= 0:
        log("      Volume je bez fragmentace – kompakce není nutná.")
        return

    if dry_run:
        log("[DRY RUN] Přehled objektů k přesunu:")
        new_offset = 0
        for obj in objects:
            log(f"  object_id={obj['object_id'][:8]}… "
                f"old_offset={obj['offset']:>10,} → new_offset={new_offset:>10,} "
                f"size={obj['size']:,}")
            new_offset += obj["size"]
        log(f"\n[DRY RUN] Kompakce by dokončena. Soubor by se zmenšil o {saved_bytes:,} B.")
        return

    # 2. Vytvoření compacted souboru
    compacted_path = VOLUMES_DIR / f"volume_{volume_id}_compacted.dat"
    log(f"[2/5] Vytvářím compacted soubor: {compacted_path}")

    try:
        compacted_path.parent.mkdir(parents=True, exist_ok=True)
        compacted_file = open(compacted_path, "wb")
    except OSError as e:
        print(f"CHYBA: Nelze vytvořit compacted soubor: {e}", file=sys.stderr)
        sys.exit(1)

    # 3. Kopírování aktivních dat do compacted souboru + aktualizace offsetů
    log(f"[3/5] Kopíruji aktivní data a aktualizuji offsety...")

    updates = []   # (object_id, new_offset) – pro pozdější aktualizaci Gateway

    try:
        new_offset    = 0
        total_written = 0

        for i, obj in enumerate(objects, 1):
            object_id  = obj["object_id"]
            old_offset = obj["offset"]
            size       = obj["size"]

            if verbose:
                print(f"  [{i:>4}/{len(objects)}] {object_id[:8]}… "
                      f"offset {old_offset:,} → {new_offset:,}  ({size:,} B)", end="")

            # Stáhnutí dat z Haystack
            try:
                data = read_from_haystack(volume_id, old_offset, size)
            except httpx.HTTPError as e:
                print(f"\n  VAROVÁNÍ: Nelze načíst objekt {object_id}: {e} – přeskakuji")
                continue

            if len(data) != size:
                print(f"\n  VAROVÁNÍ: Očekáváno {size} B, načteno {len(data)} B – přeskakuji")
                continue

            # Zápis do compacted souboru
            compacted_file.write(data)
            compacted_file.flush()

            updates.append((object_id, new_offset))
            new_offset    += size
            total_written += size

            if verbose:
                print(f"  ✓")

        compacted_file.close()
        log(f"\n      Celkem zapsáno: {total_written:,} B do {compacted_path.name}")

    except Exception as e:
        compacted_file.close()
        compacted_path.unlink(missing_ok=True)
        print(f"CHYBA při kopírování dat: {e}", file=sys.stderr)
        sys.exit(1)

    # 4. Aktualizace S3 Gateway – nové offsety
    log(f"\n[4/5] Aktualizuji S3 Gateway ({len(updates)} objektů)...")
    failed_updates = []

    for object_id, new_off in updates:
        try:
            update_object_location(object_id, volume_id, new_off)
            if verbose:
                print(f"  {object_id[:8]}… → offset={new_off:,}  ✓")
        except httpx.HTTPError as e:
            print(f"  VAROVÁNÍ: Nelze aktualizovat {object_id}: {e}")
            failed_updates.append(object_id)

    if failed_updates:
        print(f"\nVAROVÁNÍ: {len(failed_updates)} objektů se nepodařilo aktualizovat.")
        print("Compacted soubor bude zachován pro manuální opravu.")
        print(f"Compacted soubor: {compacted_path}")
        sys.exit(2)

    # 5. Výměna souborů: compacted → originální, smazání starého
    log(f"\n[5/5] Přejmenovávám compacted soubor a mažu originál...")
    try:
        # Atomické přejmenování (na stejném filesystému)
        compacted_path.replace(volume_path)
        log(f"      {compacted_path.name} → {volume_path.name}  ✓")
    except OSError as e:
        print(f"CHYBA při přejmenování: {e}", file=sys.stderr)
        print(f"Compacted soubor zůstal zachován: {compacted_path}", file=sys.stderr)
        sys.exit(1)

    log(f"\n{'='*60}")
    log(f"  Kompakce dokončena!")
    log(f"  Uvolněno: {saved_bytes:,} B ({saved_pct:.1f} %)")
    log(f"  Nová velikost: {volume_path.stat().st_size:,} B")
    log(f"{'='*60}\n")


# ======================
# KOMPAKCE VŠECH VOLUMES
# ======================

def compact_all(dry_run: bool = False):
    """Provede kompakci všech volumes v VOLUMES_DIR."""
    if not VOLUMES_DIR.exists():
        print(f"Adresář volumes neexistuje: {VOLUMES_DIR}")
        return

    volume_ids = []
    for p in sorted(VOLUMES_DIR.glob("volume_*.dat")):
        # Přeskočíme dočasné compacted soubory
        if "compacted" in p.name:
            continue
        try:
            vid = int(p.stem.split("_")[1])
            volume_ids.append(vid)
        except (IndexError, ValueError):
            pass

    if not volume_ids:
        print("Nenalezeny žádné volume soubory.")
        return

    print(f"Nalezeno {len(volume_ids)} volume(s): {volume_ids}")
    for vid in volume_ids:
        compact_volume(vid, dry_run=dry_run)


# ======================
# CLI
# ======================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Haystack Volume Compaction – defragmentace volume souborů",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Příklady:
  python compact.py --volume 1
  python compact.py --volume 1 --dry-run
  python compact.py --all
  python compact.py --volume 2 --gateway http://myserver:8000 --haystack http://myserver:8001
  python compact.py --volume 1 --volumes-dir /data/volumes
        """
    )

    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--volume", "-v",
        type=int,
        metavar="ID",
        help="Číslo volume ke kompakci (např. 1 pro volume_1.dat)"
    )
    group.add_argument(
        "--all", "-a",
        action="store_true",
        help="Kompaktuj všechny volumes v adresáři"
    )

    p.add_argument(
        "--gateway",
        default=GATEWAY_URL,
        help=f"URL S3 Gateway (výchozí: {GATEWAY_URL})"
    )
    p.add_argument(
        "--haystack",
        default=HAYSTACK_URL,
        help=f"URL Haystack Node (výchozí: {HAYSTACK_URL})"
    )
    p.add_argument(
        "--volumes-dir",
        default=str(VOLUMES_DIR),
        help=f"Adresář s volume soubory (výchozí: {VOLUMES_DIR})"
    )
    p.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Pouze zobraz co by se stalo – nic neměň"
    )
    p.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Minimální výstup"
    )

    return p


def main():
    parser  = build_parser()
    args    = parser.parse_args()

    # Přepsat globální proměnné podle CLI argumentů
    global GATEWAY_URL, HAYSTACK_URL, VOLUMES_DIR
    GATEWAY_URL  = args.gateway
    HAYSTACK_URL = args.haystack
    VOLUMES_DIR  = Path(args.volumes_dir)

    verbose = not args.quiet

    if args.all:
        compact_all(dry_run=args.dry_run)
    else:
        compact_volume(args.volume, dry_run=args.dry_run, verbose=verbose)


if __name__ == "__main__":
    main()