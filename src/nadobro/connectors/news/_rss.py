"""Tiny stdlib-only RSS / Atom parser.

We avoid adding `feedparser` as a dependency. The feeds we consume are
well-formed RSS 2.0 or Atom — this parser handles both shapes for the few
fields we need (title, link, pubDate / published, description / summary).
On any parse error or HTTP failure, returns `[]`.
"""

from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Iterable

import requests

from src.nadobro.connectors.news import NewsItem

logger = logging.getLogger(__name__)

_USER_AGENT = "NadoBro/1.0 (+https://www.nado.xyz)"
_DEFAULT_TIMEOUT = 6.0


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _text(elem: ET.Element | None) -> str:
    if elem is None or elem.text is None:
        return ""
    return elem.text.strip()


def _find(elem: ET.Element, *names: str) -> ET.Element | None:
    targets = set(names)
    for child in elem:
        if _strip_ns(child.tag) in targets:
            return child
    return None


def _parse_pubdate(raw: str) -> float | None:
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw).timestamp()
    except Exception:
        try:
            return time.mktime(time.strptime(raw[:19], "%Y-%m-%dT%H:%M:%S"))
        except Exception:
            return None


def fetch_rss_items(
    url: str,
    *,
    source: str,
    category: str,
    limit: int = 10,
    timeout: float = _DEFAULT_TIMEOUT,
    extra_headers: dict[str, str] | None = None,
) -> list[NewsItem]:
    headers = {"User-Agent": _USER_AGENT, "Accept": "application/rss+xml, application/atom+xml, text/xml, */*"}
    if extra_headers:
        headers.update(extra_headers)
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception as exc:
        logger.debug("RSS fetch failed for %s: %s", source, exc)
        return []

    entries: Iterable[ET.Element]
    root_tag = _strip_ns(root.tag)
    if root_tag == "rss":
        channel = _find(root, "channel")
        entries = list(channel) if channel is not None else []
        item_tag = "item"
    elif root_tag == "feed":
        entries = list(root)
        item_tag = "entry"
    else:
        return []

    out: list[NewsItem] = []
    for entry in entries:
        if _strip_ns(entry.tag) != item_tag:
            continue
        title = _text(_find(entry, "title"))
        link_elem = _find(entry, "link")
        link = ""
        if link_elem is not None:
            link = _text(link_elem) or link_elem.attrib.get("href", "")
        summary = _text(_find(entry, "description", "summary"))
        pub_raw = _text(_find(entry, "pubDate", "published", "updated"))
        published_at = _parse_pubdate(pub_raw)
        if not title or not link:
            continue
        out.append(
            NewsItem(
                title=title,
                url=link,
                source=source,
                category=category,
                summary=summary[:400] if summary else "",
                published_at=published_at,
            )
        )
        if len(out) >= limit:
            break
    return out
