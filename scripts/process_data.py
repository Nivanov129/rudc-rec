#!/usr/bin/env python3
"""Process Moxfield deck data into RUDC REC site data."""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA_DIR = Path.home() / ".openclaw/workspace/moxfield-scraper/data"
DECKS_DIR = DATA_DIR / "decks"
OUT_DIR = Path.home() / ".openclaw/workspace/rudc-rec/src/data"
COMMANDERS_DIR = OUT_DIR / "commanders"

NOW = datetime.now(timezone.utc)
D30 = NOW - timedelta(days=30)
D90 = NOW - timedelta(days=90)
D180 = NOW - timedelta(days=180)


def slugify(name: str) -> str:
    name = name.split(" // ")[0]  # take front face for DFCs
    s = name.lower()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"[\s]+", "-", s).strip("-")
    return s


def scryfall_image(scryfall_id: str) -> str:
    if not scryfall_id:
        return ""
    a, b = scryfall_id[0], scryfall_id[1]
    return f"https://cards.scryfall.io/normal/front/{a}/{b}/{scryfall_id}.jpg"


def classify_card(type_line: str) -> str:
    tl = type_line.lower()
    if "creature" in tl:
        return "creatures"
    if "instant" in tl:
        return "instants"
    if "sorcery" in tl:
        return "sorceries"
    if "planeswalker" in tl:
        return "planeswalkers"
    if "artifact" in tl:
        return "artifacts"
    if "enchantment" in tl:
        return "enchantments"
    if "land" in tl:
        return "lands"
    return "other"


def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def main():
    # Load banlist
    with open(DATA_DIR / "banlist.json") as f:
        banlist_raw = json.load(f)

    banned_commander_names = {c["name"] for c in banlist_raw.get("banned_as_commander", [])}
    banned_deck_names = {c["name"] for c in banlist_raw.get("banned_in_deck", [])}
    # Map name -> scryfall_id for banlist cards
    banlist_card_info = {}
    for c in banlist_raw.get("banned_as_commander", []) + banlist_raw.get("banned_in_deck", []):
        banlist_card_info[c["name"]] = c

    # Load deck list for metadata
    with open(DATA_DIR / "decks_list.json") as f:
        decks_list = json.load(f)
    deck_meta = {d["publicId"]: d for d in decks_list}

    # Process all decks
    # commander_name -> list of deck info
    commander_decks = defaultdict(list)
    # commander_name -> card info (from moxfield)
    commander_card_info = {}
    # commander_name -> { card_name -> count }
    commander_card_counts = defaultdict(lambda: defaultdict(int))
    # global card_name -> count (across all decks)
    global_card_counts = defaultdict(int)
    # card_name -> card info
    all_card_info = {}
    # total decks
    total_decks = 0

    deck_files = list(DECKS_DIR.glob("*.json"))
    print(f"Processing {len(deck_files)} deck files...")

    for fp in deck_files:
        try:
            with open(fp) as f:
                deck = json.load(f)
        except Exception as e:
            print(f"  SKIP {fp.name}: {e}")
            continue

        # Find commander(s)
        commanders = deck.get("commanders", {})
        if not commanders:
            # Some decks might have commander in sideboard
            continue

        total_decks += 1
        public_id = deck.get("publicId", fp.stem)
        created = deck.get("createdAtUtc")
        updated = deck.get("lastUpdatedAtUtc")
        deck_name = deck.get("name", "Unnamed")
        author = deck.get("createdByUser", {}).get("userName", "Unknown")

        created_dt = parse_dt(created)

        # Get all commander names
        cmdr_names = []
        for cname, cdata in commanders.items():
            card = cdata.get("card", {})
            cmdr_names.append(card.get("name", cname))
            commander_card_info[card.get("name", cname)] = card

        # Collect mainboard cards
        mainboard = deck.get("mainboard", {})
        deck_card_names = set()

        for cname, cdata in mainboard.items():
            card = cdata.get("card", {})
            card_name = card.get("name", cname)
            deck_card_names.add(card_name)
            all_card_info[card_name] = card

        # Also add commander cards to all_card_info
        for cname, cdata in commanders.items():
            card = cdata.get("card", {})
            card_name = card.get("name", cname)
            all_card_info[card_name] = card

        # Count cards per commander
        for cmdr in cmdr_names:
            commander_decks[cmdr].append({
                "name": deck_name,
                "author": author,
                "url": f"https://moxfield.com/decks/{public_id}",
                "created_at": created or "",
                "updated_at": updated or "",
                "_created_dt": created_dt,
            })
            for card_name in deck_card_names:
                commander_card_counts[cmdr][card_name] += 1

        # Global counts
        for card_name in deck_card_names:
            global_card_counts[card_name] += 1

    print(f"Total decks with commanders: {total_decks}")
    print(f"Unique commanders: {len(commander_decks)}")
    print(f"Unique cards: {len(all_card_info)}")

    # Build commanders.json
    commanders_list = []
    for cmdr_name, decks in commander_decks.items():
        card = commander_card_info.get(cmdr_name, {})
        scryfall_id = card.get("scryfall_id", "")
        deck_count = len(decks)
        deck_count_90d = sum(1 for d in decks if d["_created_dt"] and d["_created_dt"] >= D90)
        deck_count_30d = sum(1 for d in decks if d["_created_dt"] and d["_created_dt"] >= D30)

        is_banned_cmdr = cmdr_name in banned_commander_names
        is_banned_deck = cmdr_name in banned_deck_names
        if is_banned_cmdr:
            banned_type = "commander"
        elif is_banned_deck:
            banned_type = "deck"
        else:
            banned_type = None

        cmdr_data = {
            "id": slugify(cmdr_name),
            "name": cmdr_name,
            "scryfall_id": scryfall_id,
            "image_uri": scryfall_image(scryfall_id),
            "color_identity": sorted(card.get("color_identity", [])),
            "type_line": card.get("type_line", ""),
            "mana_cost": card.get("mana_cost", ""),
            "cmc": card.get("cmc", 0),
            "deck_count": deck_count,
            "deck_count_90d": deck_count_90d,
            "deck_count_30d": deck_count_30d,
            "is_banned": is_banned_cmdr or is_banned_deck,
            "banned_type": banned_type,
        }
        commanders_list.append(cmdr_data)

    commanders_list.sort(key=lambda x: -x["deck_count"])

    # Build commander detail files
    COMMANDERS_DIR.mkdir(parents=True, exist_ok=True)
    categories = ["creatures", "instants", "sorceries", "artifacts", "enchantments", "planeswalkers", "lands"]

    for cmdr in commanders_list:
        cmdr_name = cmdr["name"]
        card_counts = commander_card_counts[cmdr_name]
        n_decks = cmdr["deck_count"]

        top_cards = {cat: [] for cat in categories}

        for card_name, count in card_counts.items():
            card = all_card_info.get(card_name, {})
            type_line = card.get("type_line", "")
            cat = classify_card(type_line)
            if cat == "other":
                continue

            inclusion_pct = round(count / n_decks * 100, 1) if n_decks > 0 else 0
            global_pct = round(global_card_counts[card_name] / total_decks * 100, 1) if total_decks > 0 else 0
            synergy = round(inclusion_pct - global_pct, 1)
            sid = card.get("scryfall_id", "")

            top_cards[cat].append({
                "name": card_name,
                "scryfall_id": sid,
                "image_uri": scryfall_image(sid),
                "inclusion_pct": inclusion_pct,
                "deck_count": count,
                "synergy": synergy,
            })

        # Sort and limit
        for cat in categories:
            top_cards[cat].sort(key=lambda x: -x["inclusion_pct"])
            top_cards[cat] = top_cards[cat][:50]

        # Decks list (without internal fields)
        decks_clean = [
            {k: v for k, v in d.items() if not k.startswith("_")}
            for d in commander_decks[cmdr_name]
        ]
        decks_clean.sort(key=lambda x: x.get("updated_at", ""), reverse=True)

        detail = {
            "commander": cmdr,
            "top_cards": top_cards,
            "decks": decks_clean,
        }

        with open(COMMANDERS_DIR / f"{cmdr['id']}.json", "w") as f:
            json.dump(detail, f, ensure_ascii=False)

    # Build cards.json
    cards_list = []
    for card_name, count in global_card_counts.items():
        card = all_card_info.get(card_name, {})
        sid = card.get("scryfall_id", "")
        cards_list.append({
            "name": card_name,
            "slug": slugify(card_name),
            "scryfall_id": sid,
            "image_uri": scryfall_image(sid),
            "type_line": card.get("type_line", ""),
            "total_decks": count,
            "total_pct": round(count / total_decks * 100, 1) if total_decks > 0 else 0,
            "color_identity": sorted(card.get("color_identity", [])),
        })
    cards_list.sort(key=lambda x: -x["total_decks"])

    # Build banlist.json with image_uri
    banlist_out = {
        "banned_as_commander": [],
        "banned_in_deck": [],
    }
    for c in banlist_raw.get("banned_as_commander", []):
        sid = c.get("scryfall_id", "")
        # Try to get from all_card_info if available
        if not sid and c["name"] in all_card_info:
            sid = all_card_info[c["name"]].get("scryfall_id", "")
        entry = {**c, "image_uri": scryfall_image(sid)}
        banlist_out["banned_as_commander"].append(entry)
    for c in banlist_raw.get("banned_in_deck", []):
        sid = c.get("scryfall_id", "")
        if not sid and c["name"] in all_card_info:
            sid = all_card_info[c["name"]].get("scryfall_id", "")
        entry = {**c, "image_uri": scryfall_image(sid)}
        banlist_out["banned_in_deck"].append(entry)

    # Build meta.json
    color_dist = defaultdict(int)
    for cmdr in commanders_list:
        for c in cmdr["color_identity"]:
            color_dist[c] += cmdr["deck_count"]

    # Top 20 recent (last 6 months)
    top_recent = []
    for cmdr in commanders_list:
        cmdr_name = cmdr["name"]
        recent_count = sum(
            1 for d in commander_decks[cmdr_name]
            if d["_created_dt"] and d["_created_dt"] >= D180
        )
        if recent_count > 0:
            top_recent.append({**cmdr, "deck_count_180d": recent_count})
    top_recent.sort(key=lambda x: -x["deck_count_180d"])
    top_recent = top_recent[:20]

    meta = {
        "total_decks": total_decks,
        "total_commanders": len(commanders_list),
        "total_unique_cards": len(cards_list),
        "last_updated": NOW.strftime("%Y-%m-%d"),
        "color_distribution": dict(color_dist),
        "top_20_recent": top_recent,
    }

    # Write output files
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUT_DIR / "commanders.json", "w") as f:
        json.dump(commanders_list, f, ensure_ascii=False)
    print(f"✅ commanders.json: {len(commanders_list)} commanders")

    with open(OUT_DIR / "cards.json", "w") as f:
        json.dump(cards_list, f, ensure_ascii=False)
    print(f"✅ cards.json: {len(cards_list)} cards")

    with open(OUT_DIR / "banlist.json", "w") as f:
        json.dump(banlist_out, f, ensure_ascii=False, indent=2)
    print(f"✅ banlist.json")

    with open(OUT_DIR / "meta.json", "w") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"✅ meta.json")

    print(f"✅ {len(commanders_list)} commander detail files in commanders/")

    # File sizes
    for name in ["commanders.json", "cards.json", "banlist.json", "meta.json"]:
        p = OUT_DIR / name
        print(f"  {name}: {p.stat().st_size / 1024:.1f} KB")

    total_cmdr_size = sum(f.stat().st_size for f in COMMANDERS_DIR.glob("*.json"))
    print(f"  commanders/: {total_cmdr_size / 1024:.1f} KB total ({len(list(COMMANDERS_DIR.glob('*.json')))} files)")

    # Validate JSON
    for name in ["commanders.json", "cards.json", "banlist.json", "meta.json"]:
        with open(OUT_DIR / name) as f:
            json.load(f)
    print("\n✅ All JSON files validated successfully")
    print("🎉 Phase 1 complete!")


if __name__ == "__main__":
    main()
