#!/usr/bin/env python3
"""Extract 'date', 'id' and up to 5 URLs from 'button_links' across JSON files.
Writes two aggregated CSVs:
- with button links: Job_pipeline/data/processed/hahu_with_button_links.csv
- without button links: Job_pipeline/data/processed/hahu_no_button_links.csv
"""
from __future__ import annotations
import json
import csv
import glob
import os
import sys
from typing import Any, List


def find_urls(obj: Any) -> List[str]:
    """Recursively find values for keys named 'url' (case-insensitive) in obj.
    Returns urls in order found.
    """
    urls: List[str] = []

    def _rec(o: Any):
        if isinstance(o, dict):
            for k, v in o.items():
                if isinstance(k, str) and k.lower() == "url":
                    if isinstance(v, str) and v:
                        urls.append(v)
                _rec(v)
        elif isinstance(o, list):
            for item in o:
                _rec(item)

    _rec(obj)
    return urls


def load_json_file(path: str):
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            # try newline-delimited JSON
            f.seek(0)
            items = []
            for line in f:
                line = line.strip()
                if not line:
                    continue
                items.append(json.loads(line))
            return items


def main(raw_glob: str | None = None) -> int:
    base_dir = os.path.dirname(__file__)
    # default input: all json files in this hahu folder
    if raw_glob is None:
        raw_glob = os.path.join(base_dir, "*.json")

    out_with = os.path.join(base_dir, "hahu_with_button_links.csv")
    out_without = os.path.join(base_dir, "hahu_no_button_links.csv")

    json_paths = sorted(glob.glob(raw_glob))
    if not json_paths:
        print(f"No files matched {raw_glob}")
        return 2

    headers = ["source_file", "date", "id"] + [f"url_{i}" for i in range(1, 6)]

    with open(out_with, "w", newline="", encoding="utf-8") as f_with, \
         open(out_without, "w", newline="", encoding="utf-8") as f_without:

        writer_with = csv.DictWriter(f_with, fieldnames=headers)
        writer_without = csv.DictWriter(f_without, fieldnames=headers)
        writer_with.writeheader()
        writer_without.writeheader()

        for p in json_paths:
            try:
                items = load_json_file(p)
            except Exception as e:
                print(f"Failed to load {p}: {e}", file=sys.stderr)
                continue

            if not isinstance(items, list):
                # If file contains a dict wrapper, try to find a list inside
                # e.g., {'items': [...]} or similar
                possible = None
                for v in items.values() if isinstance(items, dict) else []:
                    if isinstance(v, list):
                        possible = v
                        break
                if possible is None:
                    print(f"File {p} did not contain a list of items; skipping.")
                    continue
                items = possible

            for it in items:
                date = it.get("date") if isinstance(it, dict) else None
                _id = it.get("id") if isinstance(it, dict) else None

                # Look for button_links key variations
                bl = None
                if isinstance(it, dict):
                    for key in ("button_links", "buttonLinks", "buttons", "button"):
                        if key in it:
                            bl = it[key]
                            break

                urls = []
                if bl is not None:
                    urls = find_urls(bl)

                row = {k: "" for k in headers}
                row["source_file"] = os.path.basename(p)
                row["date"] = date or ""
                row["id"] = _id or ""
                for i, u in enumerate(urls[:5], start=1):
                    row[f"url_{i}"] = u

                if urls:
                    writer_with.writerow(row)
                else:
                    writer_without.writerow(row)

    print(f"Wrote with-links: {out_with}")
    print(f"Wrote without-links: {out_without}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
