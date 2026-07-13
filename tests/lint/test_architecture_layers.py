"""Architecture guard: the package import graph may only shrink.

The 2026-07 decomposition split the old 108-file services/ bucket into domain
packages (core, quant, venue, market_data, llm, trading, strategy, users,
portfolio, vault, notify, runtime) and removed every upward edge (nothing but
handlers/ imports handlers/; engine/ reaches only venue/quant/utils; leaf
packages import nothing above themselves).

This test pins the resulting package->package MODULE-LEVEL import edge set.
Function-local (lazy) imports and TYPE_CHECKING blocks are exempt — the guard
protects import-time layering, which is what rots silently.

If your change adds an edge that fails here, prefer moving the code to the
right layer or passing data/callables across it. If the new edge is genuinely
right, add it below with a one-line justification.
"""
from __future__ import annotations

import ast
import functools
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "src" / "nadobro"
PKG_PREFIX = "src.nadobro"

# (importing package, imported package). Root modules (config.py, db.py,
# i18n.py, market_categories.py) count as their own single-name "package".
ALLOWED_EDGES = {
    ("config", "utils"),
    ("connectors", "utils"),
    ("core", "utils"),
    ("db", "utils"),
    ("engine", "quant"),
    ("engine", "utils"),
    ("engine", "venue"),          # adapter wraps the venue client
    ("handlers", "config"),
    ("handlers", "core"),
    ("handlers", "db"),
    ("handlers", "i18n"),
    ("handlers", "llm"),
    ("handlers", "notify"),
    ("handlers", "strategy"),
    ("handlers", "trading"),
    ("handlers", "users"),
    ("handlers", "utils"),
    ("handlers", "vault"),
    ("handlers", "venue"),
    ("llm", "config"),
    ("llm", "connectors"),
    ("llm", "db"),
    ("llm", "i18n"),
    ("llm", "models"),
    ("llm", "strategy"),          # managed agent starts/stops strategies
    ("llm", "trading"),
    ("llm", "users"),
    ("llm", "utils"),
    ("market_data", "config"),
    ("market_data", "connectors"),
    ("market_data", "core"),
    ("market_data", "utils"),
    ("models", "db"),
    ("notify", "config"),
    ("notify", "core"),
    ("notify", "i18n"),
    ("notify", "models"),
    ("notify", "users"),
    ("notify", "utils"),
    ("portfolio", "db"),
    ("portfolio", "engine"),
    ("portfolio", "trading"),
    ("portfolio", "users"),
    ("portfolio", "utils"),
    ("quant", "utils"),
    ("runtime", "core"),
    ("runtime", "notify"),
    ("runtime", "trading"),
    ("runtime", "users"),
    ("runtime", "utils"),
    ("runtime", "venue"),
    ("strategy", "config"),
    ("strategy", "core"),
    ("strategy", "db"),
    ("strategy", "engine"),
    ("strategy", "llm"),          # overlay/HOWL consume the gateway
    ("strategy", "models"),
    ("strategy", "quant"),
    ("strategy", "trading"),
    ("strategy", "users"),
    ("strategy", "utils"),
    ("strategy", "venue"),
    ("trading", "config"),
    ("trading", "core"),
    ("trading", "db"),
    ("trading", "engine"),
    ("trading", "market_data"),  # copy_discovery consumes the NadoExplorer client
    ("trading", "models"),
    ("trading", "users"),
    ("trading", "utils"),
    ("trading", "venue"),
    ("users", "config"),
    ("users", "core"),
    ("users", "db"),
    ("users", "i18n"),
    ("users", "models"),
    ("users", "strategy"),        # settings expose registry defaults; unlink stops strategies
    ("users", "utils"),
    ("users", "venue"),
    ("vault", "core"),
    ("vault", "models"),
    ("vault", "users"),
    ("vault", "utils"),
    ("vault", "venue"),
    ("venue", "config"),
    ("venue", "core"),
    ("venue", "db"),
    ("venue", "quant"),
    ("venue", "trading"),         # nado_tooling reports execution-queue diagnostics
    ("venue", "users"),
    ("venue", "utils"),
}

# Packages that must never be imported from outside themselves.
HANDLERS_ONLY_FROM = {"handlers"}


def _pkg_of(dotted: str) -> str | None:
    parts = dotted.split(".")
    return parts[2] if len(parts) >= 3 else None


def _module_level_imports(tree: ast.Module):
    stmts = []
    for node in tree.body:
        if isinstance(node, ast.If):
            t = node.test
            is_tc = (isinstance(t, ast.Name) and t.id == "TYPE_CHECKING") or (
                isinstance(t, ast.Attribute) and t.attr == "TYPE_CHECKING"
            )
            if is_tc:
                continue
            stmts.extend(node.body + node.orelse)
        else:
            stmts.append(node)
    for node in stmts:
        if isinstance(node, ast.Import):
            for a in node.names:
                yield a.name
        elif isinstance(node, ast.ImportFrom) and node.module:
            yield node.module


@functools.cache  # ~1s walk+parse of the whole tree; every test here shares one pass
def _edges() -> dict[tuple[str, str], list[str]]:
    found: dict[tuple[str, str], list[str]] = {}
    for path in SRC.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(REPO_ROOT)
        dotted = ".".join(rel.with_suffix("").parts)
        if dotted.endswith(".__init__"):
            dotted = dotted[: -len(".__init__")]
        src_pkg = _pkg_of(dotted)
        if src_pkg is None:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for target in _module_level_imports(tree):
            if not target.startswith(PKG_PREFIX + "."):
                continue
            dst_pkg = _pkg_of(target)
            if dst_pkg and dst_pkg != src_pkg:
                found.setdefault((src_pkg, dst_pkg), []).append(str(rel))
    return found


def test_package_import_edges_only_shrink():
    found = _edges()
    new = {e: files for e, files in found.items() if e not in ALLOWED_EDGES}
    assert not new, (
        "new package-level import edge(s) — move the code to the right layer "
        "or justify the edge in tests/lint/test_architecture_layers.py:\n  "
        + "\n  ".join(f"{a} -> {b}  (e.g. {files[0]})" for (a, b), files in sorted(new.items()))
    )


def test_only_handlers_import_handlers():
    offenders = [
        f"{a} -> handlers ({files[0]})"
        for (a, b), files in _edges().items()
        if b == "handlers" and a not in HANDLERS_ONLY_FROM
    ]
    assert not offenders, (
        "domain/infra packages must not import the handlers layer (UI); "
        "move the shared piece into the owning domain package:\n  "
        + "\n  ".join(offenders)
    )
