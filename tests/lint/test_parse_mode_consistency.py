"""CI lint: text markup must match the parse_mode it is sent with.

Found in production (2026-06): the nightly HOWL report was built with
Markdown (*bold*, _italic_) but sent with NO parse_mode — every recipient
saw literal asterisks (the "*hdhd*" sighting). Telegram renders exactly
what you declare: MD text needs MARKDOWN/MARKDOWN_V2, HTML text needs
HTML, and either without a parse_mode prints the markup characters raw.

Heuristic, tuned for zero false positives on the current tree:
- A *builder* is any function whose string constants contain MD markers
  (``*``/`` ` ``/escape_md) or HTML tags (<b>/<code>/<i>) — docstrings
  excluded.
- At each send call (_edit_loc / reply_text / send_message /
  edit_message_text), the text payload's class is derived from inline
  builder calls plus line-ordered variable assignments within the same
  function (sequential tracking, so a later reassignment cannot taint an
  earlier send).
- Sends inside Markdown-parse-failure fallback handlers (the
  plain_text_fallback pattern) are exempt — their markup is stripped.
"""
from __future__ import annotations

import ast
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
# All bot packages: message builders live in handlers/ and the domain
# packages (llm/, strategies/, trading/, notify/, ...) since the services/
# decomposition. Scanning the whole tree keeps every builder classified.
SCAN = sorted((REPO_ROOT / "src/nadobro").rglob("*.py"))
SENDS = {"_edit_loc", "reply_text", "send_message", "edit_message_text"}


def _docstring_nodes(fn: ast.AST) -> set[int]:
    out = set()
    for node in ast.walk(fn):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            if (node.body and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                out.add(id(node.body[0].value))
    return out


def _classify_builders() -> tuple[set[str], set[str]]:
    md, html = set(), set()
    for f in SCAN:
        tree = ast.parse(f.read_text(encoding="utf-8"))
        for fn in tree.body:
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            doc_ids = _docstring_nodes(fn)
            is_md = is_html = False
            for n in ast.walk(fn):
                if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in doc_ids:
                    if "<b>" in n.value or "<code>" in n.value or "<i>" in n.value:
                        is_html = True
                    if "*" in n.value or "`" in n.value:
                        is_md = True
                if isinstance(n, ast.Name) and n.id == "escape_md":
                    is_md = True
            if is_html:
                html.add(fn.name)
            elif is_md:
                md.add(fn.name)
    return md, html


def _builder_calls(node: ast.AST, names: set[str]) -> bool:
    return any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in names
        for n in ast.walk(node)
    )


def test_send_parse_mode_matches_text_markup():
    md_fns, html_fns = _classify_builders()
    problems: list[str] = []
    for f in SCAN:
        src = f.read_text(encoding="utf-8")
        src_lines = src.splitlines()
        tree = ast.parse(src)
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            # line-ordered (assign_line, var, class) events
            events: list[tuple[int, str, str]] = []
            for n in ast.walk(fn):
                if isinstance(n, ast.Assign):
                    cls = "html" if _builder_calls(n.value, html_fns) else (
                        "md" if _builder_calls(n.value, md_fns) else "")
                    if cls:
                        for t in ast.walk(n):
                            if isinstance(t, ast.Name) and isinstance(t.ctx, ast.Store):
                                events.append((n.lineno, t.id, cls))
            events.sort()

            def var_class_at(name: str, line: int) -> str:
                cls = ""
                for ln, var, c in events:
                    if ln <= line and var == name:
                        cls = c
                return cls

            for call in ast.walk(fn):
                if not isinstance(call, ast.Call):
                    continue
                fname = call.func.attr if isinstance(call.func, ast.Attribute) else (
                    call.func.id if isinstance(call.func, ast.Name) else "")
                if fname not in SENDS:
                    continue
                # fallback-path exemption: plain_text_fallback strips markup.
                # Cover the WHOLE multi-line call plus a small margin.
                end = getattr(call, "end_lineno", call.lineno) or call.lineno
                window = "\n".join(src_lines[max(0, call.lineno - 3): end + 1])
                if "plain_text_fallback" in window:
                    continue
                pm = ""
                for kw in call.keywords:
                    if kw.arg == "parse_mode":
                        pm = ast.unparse(kw.value)
                payload = list(call.args) + [kw.value for kw in call.keywords if kw.arg in (None, "text")]
                cls = ""
                for arg in payload:
                    if _builder_calls(arg, html_fns):
                        cls = "html"
                    elif _builder_calls(arg, md_fns) and cls != "html":
                        cls = "md"
                    for nm in ast.walk(arg):
                        if isinstance(nm, ast.Name) and isinstance(nm.ctx, ast.Load):
                            vc = var_class_at(nm.id, call.lineno)
                            # HTML DOMINATES: escape_md(...) wrapping reads as
                            # "md", which masked HTML payloads piped through it
                            # (the HOWL approve/reject blind spot) — an HTML
                            # variable inside an MD-escaped send is exactly the
                            # mixed-content bug this lint exists to catch.
                            if vc == "html":
                                cls = "html"
                            elif vc and not cls:
                                cls = vc
                loc = f"{f.relative_to(REPO_ROOT)}:{call.lineno} ({fn.name})"
                if cls == "md" and not pm:
                    problems.append(f"MD text sent with NO parse_mode — literal *asterisks*: {loc}")
                elif cls == "md" and "HTML" in pm:
                    problems.append(f"MD text sent as HTML: {loc}")
                elif cls == "html" and not pm:
                    problems.append(f"HTML text sent with NO parse_mode — literal <tags>: {loc}")
                elif cls == "html" and "MARKDOWN" in pm:
                    problems.append(f"HTML text sent as Markdown: {loc}")
    assert not problems, "\n  ".join(["parse-mode mismatches (the HOWL '*hdhd*' class):"] + problems)
