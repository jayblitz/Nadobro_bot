"""Journal tests: write/read, snapshot per tick, learnings cap at 20,
session-number increment."""
from __future__ import annotations

from src.nadobro.engine.journal import LEARNINGS_CAP, Journal


def test_write_and_read(tmp_path):
    j = Journal(1, "c1", root=tmp_path)
    j.log("started")
    j.log("tick 1")
    text = j.read_journal()
    assert "started" in text and "tick 1" in text


def test_snapshot_per_tick(tmp_path):
    j = Journal(1, "c1", root=tmp_path)
    p1 = j.snapshot("decision A", "diff A")
    p2 = j.snapshot("decision B")
    assert p1.exists() and p2.exists()
    assert j.snapshot_count == 2
    assert "decision A" in p1.read_text()
    assert p1.name == "snapshot_1.md" and p2.name == "snapshot_2.md"


def test_learnings_capped_at_20(tmp_path):
    j = Journal(1, "c1", root=tmp_path)
    for i in range(25):
        j.add_learning(f"lesson {i}")
    learnings = j.read_learnings()
    assert len(learnings) == LEARNINGS_CAP == 20
    assert learnings[-1] == "lesson 24"
    assert learnings[0] == "lesson 5"  # oldest five dropped


def test_session_number_increments(tmp_path):
    j1 = Journal(1, "c1", root=tmp_path)
    assert j1.session_n == 1
    j2 = Journal(1, "c1", root=tmp_path)
    assert j2.session_n == 2
    # different controller restarts numbering
    j3 = Journal(1, "c2", root=tmp_path)
    assert j3.session_n == 1
