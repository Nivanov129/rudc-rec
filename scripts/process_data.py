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
CARDS_DIR = OUT_DIR / "cards"

NOW = datetime.now(timezone.utc)
D30 = NOW - timedelta(days=30)
D90 = NOW - timedelta(days=90)
D180 = NOW - timedelta(days=180)


def slugify(name: str) -> str:
    name = name.split(" // ")[0]
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


def make_pair_id(name1: str, name2: str) -> str:
    """Canonical pair id: sorted slugs joined by --"""
    s1, s2 = slugify(name1), slugify(name2)
    if s1 > s2:
        s1, s2 = s2, s1
    return f"{s1}--{s2}"


def is_partner_type(card: dict) -> str:
    """Returns 'partner', 'background', or 'other'"""
    type_line = card.get("type_line", "")
    oracle = card.get("oracle_text", "")
    if "Background" in type_line:
        return "background"
    if "Partner" in oracle:
        return "partner"
    # Doctor's Companion, Friends forever, etc - treat as partner
    if "Choose a Background" in oracle:
        return "background"
    return "other"


def main():
    # Load banlist
    with open(DATA_DIR / "banlist.json") as f:
        banlist_raw = json.load(f)

    banned_commander_names = {c["name"] for c in banlist_raw.get("banned_as_commander", [])}
    banned_deck_names = {c["name"] for c in banlist_raw.get("banned_in_deck", [])}

    # Load deck list for metadata
    with open(DATA_DIR / "decks_list.json") as f:
        decks_list = json.load(f)
    deck_meta = {d["publicId"]: d for d in decks_list}

    # Data structures
    commander_decks = defaultdict(list)
    commander_card_info = {}
    commander_card_counts = defaultdict(lambda: defaultdict(int))
    global_card_counts = defaultdict(int)
    all_card_info = {}
    total_decks = 0

    # Partner tracking
    # pair_key (canonical) -> list of deck info
    pair_decks = defaultdict(list)
    # pair_key -> (name1, name2) canonical names
    pair_names = {}
    # pair_key -> {card_name -> count}
    pair_card_counts = defaultdict(lambda: defaultdict(int))
    # commander_name -> set of partner names
    commander_partners = defaultdict(set)
    # Card-level: card_name -> {cmdr_id -> deck_count}
    card_commander_counts = defaultdict(lambda: defaultdict(int))

    deck_files = list(DECKS_DIR.glob("*.json"))
    print(f"Processing {len(deck_files)} deck files...")

    for fp in deck_files:
        try:
            with open(fp) as f:
                deck = json.load(f)
        except Exception as e:
            print(f"  SKIP {fp.name}: {e}")
            continue

        commanders = deck.get("commanders", {})
        if not commanders:
            continue

        total_decks += 1
        public_id = deck.get("publicId", fp.stem)
        created = deck.get("createdAtUtc")
        updated = deck.get("lastUpdatedAtUtc")
        deck_name = deck.get("name", "Unnamed")
        author = deck.get("createdByUser", {}).get("userName", "Unknown")
        created_dt = parse_dt(created)

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

        for cname, cdata in commanders.items():
            card = cdata.get("card", {})
            card_name = card.get("name", cname)
            all_card_info[card_name] = card

        deck_info = {
            "name": deck_name,
            "author": author,
            "url": f"https://moxfield.com/decks/{public_id}",
            "created_at": created or "",
            "updated_at": updated or "",
            "_created_dt": created_dt,
        }

        for cmdr in cmdr_names:
            commander_decks[cmdr].append(deck_info)
            for card_name in deck_card_names:
                commander_card_counts[cmdr][card_name] += 1

        # Track card -> commander mapping
        for cmdr in cmdr_names:
            cmdr_id = slugify(cmdr)
            for card_name in deck_card_names:
                card_commander_counts[card_name][cmdr_id] += 1

        # Partner pairs
        if len(cmdr_names) >= 2:
            n1, n2 = cmdr_names[0], cmdr_names[1]
            pair_key = make_pair_id(n1, n2)
            s1, s2 = slugify(n1), slugify(n2)
            if s1 > s2:
                n1, n2 = n2, n1
            pair_names[pair_key] = (n1, n2)
            pair_decks[pair_key].append(deck_info)
            commander_partners[n1].add(n2)
            commander_partners[n2].add(n1)
            for card_name in deck_card_names:
                pair_card_counts[pair_key][card_name] += 1

        for card_name in deck_card_names:
            global_card_counts[card_name] += 1

    print(f"Total decks: {total_decks}")
    print(f"Unique commanders: {len(commander_decks)}")
    print(f"Unique cards: {len(all_card_info)}")
    print(f"Partner pairs: {len(pair_decks)}")

    # Build commanders.json
    commanders_list = []
    cmdr_by_id = {}
    for cmdr_name, decks in commander_decks.items():
        card = commander_card_info.get(cmdr_name, {})
        scryfall_id = card.get("scryfall_id", "")
        deck_count = len(decks)
        deck_count_90d = sum(1 for d in decks if d["_created_dt"] and d["_created_dt"] >= D90)
        deck_count_30d = sum(1 for d in decks if d["_created_dt"] and d["_created_dt"] >= D30)

        is_banned_cmdr = cmdr_name in banned_commander_names
        is_banned_deck = cmdr_name in banned_deck_names
        banned_type = "commander" if is_banned_cmdr else ("deck" if is_banned_deck else None)

        has_partners = cmdr_name in commander_partners
        partners = []
        if has_partners:
            for pname in commander_partners[cmdr_name]:
                pcard = commander_card_info.get(pname, {})
                psid = pcard.get("scryfall_id", "")
                pair_key = make_pair_id(cmdr_name, pname)
                partners.append({
                    "name": pname,
                    "id": slugify(pname),
                    "image_uri": scryfall_image(psid),
                    "deck_count": len(pair_decks.get(pair_key, [])),
                })
            partners.sort(key=lambda x: -x["deck_count"])

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
            "has_partners": has_partners,
            "partners": partners,
        }
        commanders_list.append(cmdr_data)
        cmdr_by_id[slugify(cmdr_name)] = cmdr_data

    commanders_list.sort(key=lambda x: -x["deck_count"])

    # Build commander detail files
    COMMANDERS_DIR.mkdir(parents=True, exist_ok=True)
    categories = ["creatures", "instants", "sorceries", "artifacts", "enchantments", "planeswalkers", "lands"]

    def build_top_cards(card_counts, n_decks):
        top_cards = {cat: [] for cat in categories}
        for card_name, count in card_counts.items():
            card = all_card_info.get(card_name, {})
            type_line = card.get("type_line", "")
            cat = classify_card(type_line)
            if cat == "other":
                continue
            inclusion_pct = round(count / n_decks * 100, 1) if n_decks > 0 else 0
            global_pct = round(global_card_counts.get(card_name, 0) / total_decks * 100, 1) if total_decks > 0 else 0
            synergy = round(inclusion_pct - global_pct, 1)
            sid = card.get("scryfall_id", "")
            top_cards[cat].append({
                "name": card_name,
                "slug": slugify(card_name),
                "scryfall_id": sid,
                "image_uri": scryfall_image(sid),
                "inclusion_pct": inclusion_pct,
                "deck_count": count,
                "synergy": synergy,
            })
        for cat in categories:
            top_cards[cat].sort(key=lambda x: -x["inclusion_pct"])
            top_cards[cat] = top_cards[cat][:50]
        return top_cards

    for cmdr in commanders_list:
        cmdr_name = cmdr["name"]
        n_decks = cmdr["deck_count"]
        top_cards = build_top_cards(commander_card_counts[cmdr_name], n_decks)

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

    # Build pair detail files
    pair_count = 0
    for pair_key, (n1, n2) in pair_names.items():
        decks_list_pair = pair_decks[pair_key]
        n_decks = len(decks_list_pair)
        if n_decks == 0:
            continue
        pair_count += 1

        card1 = commander_card_info.get(n1, {})
        card2 = commander_card_info.get(n2, {})
        colors = sorted(set(card1.get("color_identity", []) + card2.get("color_identity", [])))

        c1_data = cmdr_by_id.get(slugify(n1), {})
        c2_data = cmdr_by_id.get(slugify(n2), {})

        top_cards = build_top_cards(pair_card_counts[pair_key], n_decks)

        decks_clean = [
            {k: v for k, v in d.items() if not k.startswith("_")}
            for d in decks_list_pair
        ]
        decks_clean.sort(key=lambda x: x.get("updated_at", ""), reverse=True)

        ptype = is_partner_type(card2)
        if ptype == "other":
            ptype = is_partner_type(card1)

        detail = {
            "pair_id": pair_key,
            "pair_type": ptype,
            "commander1": {
                "name": n1, "id": slugify(n1),
                "scryfall_id": card1.get("scryfall_id", ""),
                "image_uri": scryfall_image(card1.get("scryfall_id", "")),
                "color_identity": sorted(card1.get("color_identity", [])),
                "type_line": card1.get("type_line", ""),
            },
            "commander2": {
                "name": n2, "id": slugify(n2),
                "scryfall_id": card2.get("scryfall_id", ""),
                "image_uri": scryfall_image(card2.get("scryfall_id", "")),
                "color_identity": sorted(card2.get("color_identity", [])),
                "type_line": card2.get("type_line", ""),
            },
            "color_identity": colors,
            "deck_count": n_decks,
            "top_cards": top_cards,
            "decks": decks_clean,
        }
        with open(COMMANDERS_DIR / f"{pair_key}.json", "w") as f:
            json.dump(detail, f, ensure_ascii=False)

    print(f"✅ {pair_count} pair detail files generated")

    # Build card detail files
    CARDS_DIR.mkdir(parents=True, exist_ok=True)
    cards_list = []
    card_pages_count = 0

    for card_name, count in global_card_counts.items():
        card = all_card_info.get(card_name, {})
        sid = card.get("scryfall_id", "")
        slug = slugify(card_name)
        total_pct = round(count / total_decks * 100, 1) if total_decks > 0 else 0

        card_entry = {
            "name": card_name,
            "slug": slug,
            "scryfall_id": sid,
            "image_uri": scryfall_image(sid),
            "type_line": card.get("type_line", ""),
            "total_decks": count,
            "total_pct": total_pct,
            "color_identity": sorted(card.get("color_identity", [])),
        }
        cards_list.append(card_entry)

        # Generate individual card JSON for cards with >= 3 decks
        if count >= 3:
            card_pages_count += 1
            # Build commanders list for this card
            card_cmdrs = []
            for cmdr_id, cmdr_count in card_commander_counts[card_name].items():
                cmdr_data = cmdr_by_id.get(cmdr_id)
                if not cmdr_data:
                    continue
                cmdr_n_decks = cmdr_data["deck_count"]
                inclusion_pct = round(cmdr_count / cmdr_n_decks * 100, 1) if cmdr_n_decks > 0 else 0
                global_pct = total_pct
                synergy = round(inclusion_pct - global_pct, 1)
                card_cmdrs.append({
                    "name": cmdr_data["name"],
                    "id": cmdr_id,
                    "image_uri": cmdr_data["image_uri"],
                    "inclusion_pct": inclusion_pct,
                    "synergy": synergy,
                    "deck_count": cmdr_count,
                })
            card_cmdrs.sort(key=lambda x: -x["deck_count"])
            card_cmdrs = card_cmdrs[:50]

            card_detail = {
                **card_entry,
                "commanders": card_cmdrs,
            }
            with open(CARDS_DIR / f"{slug}.json", "w") as f:
                json.dump(card_detail, f, ensure_ascii=False)

    cards_list.sort(key=lambda x: -x["total_decks"])

    # Build banlist.json
    banlist_out = {"banned_as_commander": [], "banned_in_deck": []}
    for c in banlist_raw.get("banned_as_commander", []):
        sid = c.get("scryfall_id", "")
        if not sid and c["name"] in all_card_info:
            sid = all_card_info[c["name"]].get("scryfall_id", "")
        banlist_out["banned_as_commander"].append({**c, "image_uri": scryfall_image(sid)})
    for c in banlist_raw.get("banned_in_deck", []):
        sid = c.get("scryfall_id", "")
        if not sid and c["name"] in all_card_info:
            sid = all_card_info[c["name"]].get("scryfall_id", "")
        banlist_out["banned_in_deck"].append({**c, "image_uri": scryfall_image(sid)})

    # Build meta.json
    color_dist = defaultdict(int)
    for cmdr in commanders_list:
        for c in cmdr["color_identity"]:
            color_dist[c] += cmdr["deck_count"]

    top_recent = []
    for cmdr in commanders_list:
        cmdr_name = cmdr["name"]
        recent_count = sum(1 for d in commander_decks[cmdr_name] if d["_created_dt"] and d["_created_dt"] >= D180)
        if recent_count > 0:
            top_recent.append({**cmdr, "deck_count_180d": recent_count})
    top_recent.sort(key=lambda x: -x["deck_count_180d"])
    top_recent = top_recent[:20]

    meta = {
        "total_decks": total_decks,
        "total_commanders": len(commanders_list),
        "total_unique_cards": len(cards_list),
        "total_pairs": pair_count,
        "total_card_pages": card_pages_count,
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

    # Build search index (lightweight)
    search_index = []
    for cmdr in commanders_list[:500]:
        search_index.append({"n": cmdr["name"], "id": cmdr["id"], "t": "c", "d": cmdr["deck_count"], "img": cmdr["image_uri"]})
    for card in cards_list[:500]:
        search_index.append({"n": card["name"], "id": card["slug"], "t": "k", "d": card["total_decks"], "img": card["image_uri"]})
    with open(OUT_DIR / "search-index.json", "w") as f:
        json.dump(search_index, f, ensure_ascii=False)
    print(f"✅ search-index.json: {len(search_index)} entries")

    print(f"✅ {len(commanders_list)} commander detail files")
    print(f"✅ {pair_count} pair detail files")
    print(f"✅ {card_pages_count} card detail files")

    # File sizes
    for name in ["commanders.json", "cards.json", "banlist.json", "meta.json", "search-index.json"]:
        p = OUT_DIR / name
        print(f"  {name}: {p.stat().st_size / 1024:.1f} KB")

    total_cmdr_size = sum(f.stat().st_size for f in COMMANDERS_DIR.glob("*.json"))
    print(f"  commanders/: {total_cmdr_size / 1024:.1f} KB total ({len(list(COMMANDERS_DIR.glob('*.json')))} files)")

    total_cards_size = sum(f.stat().st_size for f in CARDS_DIR.glob("*.json"))
    print(f"  cards/: {total_cards_size / 1024:.1f} KB total ({len(list(CARDS_DIR.glob('*.json')))} files)")

    # Validate
    for name in ["commanders.json", "cards.json", "banlist.json", "meta.json"]:
        with open(OUT_DIR / name) as f:
            json.load(f)
    print("\n✅ All JSON validated")
    print("🎉 Phase 3 data processing complete!")


if __name__ == "__main__":
    main()
