"""Operational journal — file-backed per-session log at
``<root>/<user_id>/<controller_id>/session_<n>/journal.md`` (default root
``~/.nadobro/sessions``), with per-tick ``snapshots/snapshot_<k>.md`` and a
cross-session ``learnings.md`` capped at 20 entries.

For ops debugging only; NOT an LLM memory.

Implemented in Phase 1.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import List, Optional

DEFAULT_ROOT = Path(os.path.expanduser("~/.nadobro/sessions"))
LEARNINGS_CAP = 20


class Journal:
    def __init__(
        self,
        user_id: int,
        controller_id: str,
        session_n: Optional[int] = None,
        root: Optional[os.PathLike] = None,
    ) -> None:
        self.user_id = user_id
        self.controller_id = controller_id
        self.root = Path(root) if root is not None else DEFAULT_ROOT
        self.session_n = session_n if session_n is not None else self._next_session_n()
        self.session_dir = (
            self.root / str(user_id) / controller_id / f"session_{self.session_n}"
        )
        self.snapshots_dir = self.session_dir / "snapshots"
        self.journal_path = self.session_dir / "journal.md"
        self.learnings_path = self.root / str(user_id) / controller_id / "learnings.md"
        self._snapshot_k = 0
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        if not self.journal_path.exists():
            self.journal_path.write_text(
                f"# Session {self.session_n} — controller {self.controller_id}\n\n",
                encoding="utf-8",
            )

    def _ctrl_dir(self) -> Path:
        return self.root / str(self.user_id) / self.controller_id

    def _next_session_n(self) -> int:
        base = self._ctrl_dir()
        if not base.exists():
            return 1
        nums: List[int] = []
        for p in base.glob("session_*"):
            tail = p.name.split("_")[-1]
            if tail.isdigit():
                nums.append(int(tail))
        return (max(nums) + 1) if nums else 1

    def log(self, message: str) -> None:
        ts = time.strftime("%Y-%m-%dT%H:%M:%S")
        with self.journal_path.open("a", encoding="utf-8") as f:
            f.write(f"- {ts} {message}\n")

    def snapshot(self, decision: str, executor_diff: str = "") -> Path:
        self._snapshot_k += 1
        ts = time.strftime("%Y-%m-%dT%H:%M:%S")
        path = self.snapshots_dir / f"snapshot_{self._snapshot_k}.md"
        path.write_text(
            f"# Snapshot {self._snapshot_k} — {ts}\n\n"
            f"## Decision\n{decision}\n\n"
            f"## Executor diff\n{executor_diff}\n",
            encoding="utf-8",
        )
        return path

    @property
    def snapshot_count(self) -> int:
        return self._snapshot_k

    def read_journal(self) -> str:
        return self.journal_path.read_text(encoding="utf-8")

    def read_learnings(self) -> List[str]:
        if not self.learnings_path.exists():
            return []
        out: List[str] = []
        for ln in self.learnings_path.read_text(encoding="utf-8").splitlines():
            ln = ln.strip()
            if not ln:
                continue
            out.append(ln[2:] if ln.startswith("- ") else ln)
        return out

    def add_learning(self, text: str) -> None:
        learnings = self.read_learnings()
        learnings.append(text)
        learnings = learnings[-LEARNINGS_CAP:]
        self.learnings_path.parent.mkdir(parents=True, exist_ok=True)
        self.learnings_path.write_text(
            "\n".join(f"- {item}" for item in learnings) + "\n", encoding="utf-8"
        )
