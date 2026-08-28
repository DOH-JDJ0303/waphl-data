#!/usr/bin/env python3
"""Shared file-type classification.

Used by prod2res/gather (typing S3 keys during ingest) and by the inspect
dashboard (typing rows from the files table's `origin` column). Keep the
matching semantics here so the two can't drift apart.
"""

import re
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Tuple

DEFAULT_TYPE = "other"
READS_TYPE = "raw_reads"
PATTERN_KEYS = ("pattern", "patterns")
READS_RX = re.compile(r"\.(fastq|fq)(\.gz)?$", re.IGNORECASE)

# Type values that mean "never successfully classified" and should be
# re-derived rather than trusted.
UNCLASSIFIED_TYPES = frozenset({"", DEFAULT_TYPE, "none", "null", "nan", "na", "unknown"})


def is_unclassified(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip().lower()
    return text in UNCLASSIFIED_TYPES


def compile_glob(pattern: str) -> re.Pattern:
    """Glob-ish -> anchored regex. '*' any run, '?' one char, rest literal."""
    buf = []
    for ch in pattern:
        if ch == "*":
            buf.append(".*")
        elif ch == "?":
            buf.append(".")
        else:
            buf.append(re.escape(ch))
    return re.compile("^" + "".join(buf) + "$")


def iter_patterns(spec: Dict[str, Any]) -> List[str]:
    """Every pattern in one reportable_files entry, in order, deduped.

    Accepts:
        {"pattern":  "*.tsv"}
        {"pattern":  ["*.tsv", "*_summary.txt"]}
        {"patterns": ["*.tsv", "*_summary.txt"]}   # alias
    """
    out: List[str] = []
    for key in PATTERN_KEYS:
        val = spec.get(key)
        if isinstance(val, str):
            val = [val]
        if isinstance(val, (list, tuple)):
            out.extend(p for p in val if isinstance(p, str) and p.strip())
    seen, ordered = set(), []
    for p in out:
        p = p.strip()
        if p not in seen:
            seen.add(p)
            ordered.append(p)
    return ordered


class FilePatternMatcher:
    """Match a basename against a scheme's reportable_files.

    Entries are tried in scheme order, and each entry's patterns in list
    order; the first hit wins. `skipped` collects entries that carried no
    usable pattern, so callers can surface a warning instead of silently
    under-matching.
    """

    def __init__(self, pattern_defs: Optional[Iterable[Dict[str, Any]]] = None):
        self.patterns: List[Tuple[re.Pattern, Dict[str, Any]]] = []
        self.skipped: List[Any] = []
        for spec in pattern_defs or []:
            if not isinstance(spec, dict):
                self.skipped.append(spec)
                continue
            pats = iter_patterns(spec)
            if not pats:
                self.skipped.append(spec)
                continue
            meta = {"type": spec.get("type", DEFAULT_TYPE)}
            for pat in pats:
                self.patterns.append((compile_glob(pat), meta))

    def __bool__(self) -> bool:
        return bool(self.patterns)

    def __len__(self) -> int:
        return len(self.patterns)

    def match(self, filename: str) -> Optional[Dict[str, Any]]:
        """Metadata dict for the first matching pattern, else None."""
        for rx, meta in self.patterns:
            if rx.match(filename):
                return meta
        return None

    def match_type(self, filename: str) -> Optional[str]:
        """Type string for the first match, else None. None means no
        pattern matched, which is distinct from an entry whose declared
        type happens to be 'other'."""
        hit = self.match(filename)
        return hit["type"] if hit else None

    def types(self) -> List[str]:
        """Declared types, deduped, in scheme order."""
        seen, ordered = set(), []
        for _, meta in self.patterns:
            t = meta["type"]
            if t not in seen:
                seen.add(t)
                ordered.append(t)
        return ordered


def parse_uri(uri: str) -> Tuple[str, str]:
    """'s3://bucket/a//b/c.txt' -> ('bucket', 'a/b/c.txt'). Raises ValueError."""
    if not isinstance(uri, str) or not uri.strip():
        raise ValueError(f"Empty S3 URI: {uri!r}")
    parsed = urllib.parse.urlparse(uri.strip())
    if parsed.scheme not in ("s3", ""):
        raise ValueError(f"Unsupported URI scheme: {uri}")
    bucket = parsed.netloc
    key = re.sub(r"/{2,}", "/", parsed.path.lstrip("/"))
    if not bucket or not key:
        raise ValueError(f"Malformed S3 URI: {uri}")
    return bucket, key


def uri_key(uri: str) -> str:
    """Key portion of an S3 URI, or '' if it can't be parsed."""
    try:
        return parse_uri(uri)[1]
    except ValueError:
        return ""


def classify_key(
    key: str,
    run: str,
    matcher: FilePatternMatcher,
    reads_by_extension: bool = False,
) -> str:
    """Type for one S3 key. Basename-only matching, as in gather."""
    if not key:
        return DEFAULT_TYPE
    if run and f"{run}/reads/" in key:
        return READS_TYPE
    basename = key.rsplit("/", 1)[-1]
    if reads_by_extension and READS_RX.search(basename):
        return READS_TYPE
    return matcher.match_type(basename) or DEFAULT_TYPE


def classify_origin(
    origin: str,
    run: str,
    matcher: FilePatternMatcher,
    reads_by_extension: bool = False,
) -> str:
    """Same as classify_key, but takes the full URI from the origin column."""
    return classify_key(uri_key(origin), run, matcher, reads_by_extension)