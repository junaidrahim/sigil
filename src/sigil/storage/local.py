"""Local JSON file storage backend."""

from __future__ import annotations

import dataclasses
import json
import logging
from datetime import datetime
from pathlib import Path

from sigil.config import SIGIL_DIR
from sigil.models import Message, Session, Snapshot, ToolUse
from sigil.storage.base import StorageBackend

logger = logging.getLogger(__name__)

SNAPSHOTS_DIR = SIGIL_DIR / "snapshots"


class LocalStorage(StorageBackend):
    """Stores snapshots as individual JSON files in ~/.sigil/snapshots/."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = base_dir or SNAPSHOTS_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save_snapshot(self, snapshot: Snapshot) -> str:
        path = self.base_dir / f"{snapshot.snapshot_id}.json"
        data = dataclasses.asdict(snapshot)
        path.write_text(json.dumps(data, indent=2, default=str))
        return snapshot.snapshot_id

    def list_snapshots(
        self,
        source: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[Snapshot]:
        snapshots: list[Snapshot] = []
        for path in sorted(self.base_dir.glob("*.json")):
            snap = self._load_file(path)
            if snap is None:
                continue
            if source and snap.source != source:
                continue
            try:
                pushed = datetime.fromisoformat(snap.pushed_at)
            except (ValueError, TypeError):
                continue
            if since and pushed < since:
                continue
            if until and pushed > until:
                continue
            snapshots.append(snap)
        return snapshots

    def get_snapshot(self, snapshot_id: str) -> Snapshot | None:
        path = self.base_dir / f"{snapshot_id}.json"
        if not path.exists():
            return None
        return self._load_file(path)

    def _load_file(self, path: Path) -> Snapshot | None:
        try:
            data = json.loads(path.read_text())
            sessions = []
            for s in data.get("sessions", []):
                messages = [Message(**m) for m in s.get("messages", [])]
                tool_uses = [ToolUse(**t) for t in s.get("tool_uses", [])]
                sessions.append(
                    Session(
                        session_id=s["session_id"],
                        source=s["source"],
                        device=s["device"],
                        started_at=s.get("started_at"),
                        ended_at=s.get("ended_at"),
                        messages=messages,
                        tool_uses=tool_uses,
                        input_tokens=s.get("input_tokens", 0),
                        output_tokens=s.get("output_tokens", 0),
                        total_tokens=s.get("total_tokens", 0),
                        project_path=s.get("project_path"),
                        model=s.get("model"),
                        raw_metadata=s.get("raw_metadata", {}),
                    )
                )
            snap = Snapshot(
                snapshot_id=data["snapshot_id"],
                pushed_at=data["pushed_at"],
                source=data.get("source", ""),
                device=data.get("device", ""),
                session_count=data.get("session_count", 0),
                sessions=sessions,
            )
            return snap
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning("Failed to load snapshot %s: %s", path, e)
            return None
