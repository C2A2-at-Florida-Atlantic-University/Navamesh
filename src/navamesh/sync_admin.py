"""CLI to inspect and purge the cloud-sync dead-letter queue.

Entry point: ``navamesh-sync-admin`` (see pyproject.toml).

Examples (on the Pi, inside the ingestor container):
    docker compose exec ingestor navamesh-sync-admin count
    docker compose exec ingestor navamesh-sync-admin list
    docker compose exec ingestor navamesh-sync-admin purge --yes
"""
import argparse
import json
import os
from datetime import datetime, timezone
from typing import Optional, Sequence

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(usecwd=True))

from navamesh.mqtt_to_db import CloudSyncQueue


def _node_id(payload_str: str) -> str:
    try:
        return str(json.loads(payload_str).get("node_id", "?"))
    except Exception:
        return "<malformed>"


def _fmt_ts(ts: Optional[int]) -> str:
    if ts is None:
        return "-"
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return str(ts)


def _parse_payload(payload_str: str):
    try:
        return json.loads(payload_str)
    except Exception:
        return payload_str  # keep raw string if it isn't valid JSON


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="navamesh-sync-admin",
        description="Inspect or purge the cloud-sync dead-letter queue.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("count", help="Print the number of dead-lettered rows.")
    p_list = sub.add_parser("list", help="List dead-lettered rows.")
    p_list.add_argument("--limit", type=int, default=100)
    p_list.add_argument(
        "--json", action="store_true",
        help="Emit full rows (incl. payload) as JSON with UTC timestamps.",
    )
    p_list.add_argument(
        "--show-payload", action="store_true",
        help="Print the complete stored payload under each row (text mode).",
    )
    p_purge = sub.add_parser("purge", help="Delete all dead-lettered rows.")
    p_purge.add_argument("--yes", action="store_true", help="Skip confirmation prompt.")
    args = parser.parse_args(argv)

    queue_path = os.getenv("SYNC_QUEUE_PATH", "cloud_sync_queue.db")
    queue = CloudSyncQueue(queue_path)
    try:
        if args.cmd == "count":
            print(queue.count_dead_letters())
        elif args.cmd == "list":
            rows = queue.list_dead_letters(limit=args.limit)
            if args.json:
                out = [
                    {
                        "id": rid,
                        "orig_id": orig_id,
                        "queued_at": queued_at,
                        "queued_at_utc": _fmt_ts(queued_at),
                        "failed_at": failed_at,
                        "failed_at_utc": _fmt_ts(failed_at),
                        "target": target,
                        "error": error,
                        "payload": _parse_payload(payload),
                    }
                    for (rid, orig_id, queued_at, failed_at, target, error, payload) in rows
                ]
                print(json.dumps(out, indent=2))
            else:
                if not rows:
                    print("(no dead-lettered rows)")
                for (rid, orig_id, queued_at, failed_at, target, error, payload) in rows:
                    print(
                        f"#{rid} orig_id={orig_id} "
                        f"queued_at={_fmt_ts(queued_at)} failed_at={_fmt_ts(failed_at)} "
                        f"target={target} node={_node_id(payload)} error={error}"
                    )
                    if args.show_payload:
                        print(f"    payload={payload}")
        elif args.cmd == "purge":
            if not args.yes:
                resp = input("Delete ALL dead-lettered rows? [y/N] ").strip().lower()
                if resp not in ("y", "yes"):
                    print("Aborted.")
                    return 1
            print(f"Purged {queue.purge_dead_letters()} dead-lettered rows.")
    finally:
        queue.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
