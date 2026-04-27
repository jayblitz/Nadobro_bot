"""Pinecone vector store for semantic search across knowledge base, X findings, and Q&A history.

Uses OpenAI text-embedding-3-small for embeddings and Pinecone serverless for storage.
Degrades gracefully to keyword search when Pinecone is not configured.
"""

import hashlib
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Clients (lazy-init) ─────────────────────────────────────────────

_pinecone_index = None
_openai_client = None
_initialised = False

EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSION = 1536

KNOWLEDGE_FILE = Path(__file__).parent.parent / "data" / "nado_knowledge.txt"

# Namespace constants
NS_KNOWLEDGE = "knowledge"
NS_X_FINDINGS = "x_findings"
NS_QA_HISTORY = "qa_history"

# Dedup threshold — cosine similarity above this means "same question"
QA_DEDUP_THRESHOLD = 0.92


def _read_attr_or_key(obj, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _get_openai_client():
    global _openai_client
    if _openai_client is not None:
        return _openai_client
    from src.nadobro.config import OPENAI_API_KEY
    if not OPENAI_API_KEY:
        return None
    from openai import OpenAI
    _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client


def _get_pinecone_index():
    global _pinecone_index, _initialised
    if _initialised:
        return _pinecone_index
    _initialised = True

    from src.nadobro.config import PINECONE_API_KEY, PINECONE_INDEX_NAME
    if not PINECONE_API_KEY:
        logger.info("PINECONE_API_KEY not set — vector store disabled, using keyword fallback")
        return None

    try:
        from pinecone import Pinecone
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index_name = PINECONE_INDEX_NAME or "nadobro"

        listed = pc.list_indexes()
        existing: list[str] = []
        if hasattr(listed, "names"):
            try:
                existing = list(listed.names())
            except Exception:
                existing = []
        elif isinstance(listed, list):
            for idx in listed:
                name = _read_attr_or_key(idx, "name")
                if name:
                    existing.append(str(name))
        if index_name not in existing:
            from pinecone import ServerlessSpec
            pc.create_index(
                name=index_name,
                dimension=EMBEDDING_DIMENSION,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )
            logger.info("Created Pinecone index: %s", index_name)

        _pinecone_index = pc.Index(index_name)
        logger.info("Connected to Pinecone index: %s", index_name)
        return _pinecone_index
    except ImportError as e:
        logger.warning("Pinecone SDK unavailable — vector store disabled: %s", e)
        _pinecone_index = None
        return None
    except Exception as e:
        logger.warning("Pinecone init failed — vector store disabled: %s", e)
        _pinecone_index = None
        return None


def is_available() -> bool:
    """Check whether vector store is ready."""
    return _get_pinecone_index() is not None and _get_openai_client() is not None


# ── Embedding helpers ────────────────────────────────────────────────

def embed_text(text: str) -> Optional[list[float]]:
    """Embed a single text string. Returns None on failure."""
    client = _get_openai_client()
    if not client:
        return None
    try:
        resp = client.embeddings.create(model=EMBEDDING_MODEL, input=[text])
        return resp.data[0].embedding
    except Exception:
        logger.warning("Embedding failed", exc_info=True)
        return None


def embed_batch(texts: list[str]) -> Optional[list[list[float]]]:
    """Embed a batch of texts. Returns None on failure."""
    client = _get_openai_client()
    if not client or not texts:
        return None
    try:
        resp = client.embeddings.create(model=EMBEDDING_MODEL, input=texts)
        return [d.embedding for d in sorted(resp.data, key=lambda d: d.index)]
    except Exception:
        logger.warning("Batch embedding failed", exc_info=True)
        return None


# ── Search ───────────────────────────────────────────────────────────

def search_similar(query: str, top_k: int = 5, namespace: Optional[str] = None) -> list[dict]:
    """Semantic search across vector store.

    Returns list of dicts: [{id, score, text, metadata}, ...]
    """
    index = _get_pinecone_index()
    if not index:
        return []

    vec = embed_text(query)
    if not vec:
        return []

    try:
        kwargs = {"vector": vec, "top_k": top_k, "include_metadata": True}
        if namespace:
            kwargs["namespace"] = namespace
        results = index.query(**kwargs)

        hits = []
        matches = _read_attr_or_key(results, "matches", []) or []
        for match in matches:
            meta = _read_attr_or_key(match, "metadata", {}) or {}
            hits.append({
                "id": _read_attr_or_key(match, "id", ""),
                "score": float(_read_attr_or_key(match, "score", 0) or 0),
                "text": _read_attr_or_key(meta, "text", ""),
                "title": _read_attr_or_key(meta, "title", ""),
                "source": _read_attr_or_key(meta, "source", ""),
                "namespace": namespace or "default",
            })
        return hits
    except Exception:
        logger.warning("Pinecone search failed", exc_info=True)
        return []


# ── Knowledge base indexing ──────────────────────────────────────────

_kb_indexed_at: float = 0.0
KB_REINDEX_INTERVAL = 3600  # 1 hour


def _parse_knowledge_sections(text: str) -> list[dict]:
    """Split knowledge base text into sections by ## headers."""
    import re
    sections = []
    parts = re.split(r'\n(?=##\s)', text)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        lines = part.split('\n', 1)
        title = lines[0].lstrip('#').strip()
        body = lines[1].strip() if len(lines) > 1 else ""
        sections.append({"title": title, "body": body, "text": part})
    return sections


def _section_id(title: str) -> str:
    """Deterministic ID for a KB section."""
    return f"kb_{hashlib.md5(title.encode()).hexdigest()[:12]}"


def index_knowledge_base(force: bool = False):
    """Index nado_knowledge.txt into Pinecone. Skips if recently done."""
    global _kb_indexed_at

    if not force and (time.time() - _kb_indexed_at) < KB_REINDEX_INTERVAL:
        return

    index = _get_pinecone_index()
    if not index:
        return

    try:
        kb_text = KNOWLEDGE_FILE.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("Knowledge file not found: %s", KNOWLEDGE_FILE)
        return

    sections = _parse_knowledge_sections(kb_text)
    if not sections:
        return

    texts = [s["text"][:8000] for s in sections]
    embeddings = embed_batch(texts)
    if not embeddings:
        return

    vectors = []
    for sec, emb in zip(sections, embeddings):
        vectors.append({
            "id": _section_id(sec["title"]),
            "values": emb,
            "metadata": {
                "title": sec["title"],
                "text": sec["text"][:8000],
                "source": "knowledge_base",
            },
        })

    # Upsert in batches of 50
    for i in range(0, len(vectors), 50):
        batch = vectors[i:i + 50]
        index.upsert(vectors=batch, namespace=NS_KNOWLEDGE)

    _kb_indexed_at = time.time()
    logger.info("Indexed %d KB sections into Pinecone", len(vectors))


# ── X findings indexing ──────────────────────────────────────────────

def index_x_finding(finding: dict):
    """Store an edge/promotion finding from X scans.

    finding: {type, title, detail, source_url, found_at}
    """
    index = _get_pinecone_index()
    if not index:
        return

    text = f"{finding.get('title', '')} {finding.get('detail', '')}"
    vec = embed_text(text)
    if not vec:
        return

    finding_id = f"xf_{hashlib.md5(text.encode()).hexdigest()[:12]}"
    try:
        index.upsert(
            vectors=[{
                "id": finding_id,
                "values": vec,
                "metadata": {
                    "title": finding.get("title", ""),
                    "text": finding.get("detail", ""),
                    "source": finding.get("source_url", ""),
                    "finding_type": finding.get("type", "general"),
                    "found_at": finding.get("found_at", time.time()),
                },
            }],
            namespace=NS_X_FINDINGS,
        )
    except Exception:
        logger.warning("Failed to index X finding", exc_info=True)


def index_x_findings_batch(findings: list[dict]):
    """Batch upsert X findings."""
    index = _get_pinecone_index()
    if not index or not findings:
        return

    texts = [f"{f.get('title', '')} {f.get('detail', '')}" for f in findings]
    embeddings = embed_batch(texts)
    if not embeddings:
        return

    vectors = []
    for finding, emb, text in zip(findings, embeddings, texts):
        finding_id = f"xf_{hashlib.md5(text.encode()).hexdigest()[:12]}"
        vectors.append({
            "id": finding_id,
            "values": emb,
            "metadata": {
                "title": finding.get("title", ""),
                "text": finding.get("detail", ""),
                "source": finding.get("source_url", ""),
                "finding_type": finding.get("type", "general"),
                "found_at": finding.get("found_at", time.time()),
            },
        })

    for i in range(0, len(vectors), 50):
        batch = vectors[i:i + 50]
        index.upsert(vectors=batch, namespace=NS_X_FINDINGS)

    logger.info("Indexed %d X findings into Pinecone", len(vectors))


# ── Q&A dedup + indexing ─────────────────────────────────────────────

def index_qa_if_unique(question: str, answer: str) -> bool:
    """Index a Q&A pair if the question is unique (cosine < threshold).

    Returns True if indexed, False if duplicate or error.
    """
    index = _get_pinecone_index()
    if not index:
        return False

    vec = embed_text(question)
    if not vec:
        return False

    # Check for duplicates
    try:
        results = index.query(
            vector=vec,
            top_k=1,
            namespace=NS_QA_HISTORY,
            include_metadata=True,
        )
        matches = _read_attr_or_key(results, "matches", []) or []
        first = matches[0] if matches else None
        first_score = float(_read_attr_or_key(first, "score", 0) or 0)
        if first and first_score >= QA_DEDUP_THRESHOLD:
            # Near-duplicate — increment query count on existing entry
            existing_id = _read_attr_or_key(first, "id")
            existing_meta = _read_attr_or_key(first, "metadata", {}) or {}
            query_count = int(_read_attr_or_key(existing_meta, "query_count", 1) or 1) + 1
            existing_meta["query_count"] = query_count
            # Use the new question's embedding (close enough) since query
            # doesn't return values by default.
            index.upsert(
                vectors=[{
                    "id": existing_id,
                    "values": vec,
                    "metadata": existing_meta,
                }],
                namespace=NS_QA_HISTORY,
            )
            return False
    except Exception:
        logger.warning("QA dedup check failed", exc_info=True)

    # Unique question — index it
    qa_id = f"qa_{hashlib.md5(question.encode()).hexdigest()[:12]}"
    try:
        index.upsert(
            vectors=[{
                "id": qa_id,
                "values": vec,
                "metadata": {
                    "title": question[:200],
                    "text": answer[:4000],
                    "source": "qa_history",
                    "query_count": 1,
                    "indexed_at": time.time(),
                },
            }],
            namespace=NS_QA_HISTORY,
        )
        return True
    except Exception:
        logger.warning("QA indexing failed", exc_info=True)
        return False


def search_qa_history(query: str, top_k: int = 3) -> list[dict]:
    """Search past Q&A pairs for similar questions."""
    return search_similar(query, top_k=top_k, namespace=NS_QA_HISTORY)
