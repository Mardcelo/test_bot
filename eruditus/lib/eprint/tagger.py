import re

from config import DBNAME, DISCUSSION_SETTINGS_COLLECTION, EPRINT_TRACKED_TOPICS, MONGO

TAG_PATTERNS: dict[str, tuple[str, ...]] = {
    "isogeny": (
        r"\bisogen(?:y|ies|ous)\b",
        r"\bsidh\b",
        r"\bsike\b",
        r"\bcsidh\b",
        r"\bcsurf\b",
        r"\bsqisign\b",
        r"\bsupersingular\b",
    ),
    "pqc": (
        r"\bpqc\b",
        r"\bpost[- ]quantum\b",
        r"\bquantum[- ]safe\b",
        r"\bml-kem\b",
        r"\bml-dsa\b",
        r"\bslh-dsa\b",
        r"\bkyber\b",
        r"\bdilithium\b",
        r"\bfalcon\b",
        r"\bhqc\b",
        r"\bmceliece\b",
        r"\bbike\b",
        r"\bsaber\b",
        r"\bfrodokem\b",
        r"\bntru\b",
        r"\bsqisign\b",
        r"\bmayo\b",
    ),
    "lattice": (
        r"\blattice(?:s)?\b",
        r"\blwe\b",
        r"\bring[- ]lwe\b",
        r"\bmodule[- ]lwe\b",
        r"\bsis\b",
        r"\bntru\b",
        r"\bdilithium\b",
        r"\bfalcon\b",
        r"\bkyber\b",
        r"\bfrodokem\b",
        r"\bsaber\b",
    ),
}


def normalize_topic_name(value: str) -> str:
    """Normalize a tracked topic for matching and storage."""
    return " ".join(value.lower().replace("_", " ").split())


def get_tracked_topics() -> tuple[str, ...]:
    """Return the current tracked topics, falling back to env defaults."""
    settings = MONGO[DBNAME][DISCUSSION_SETTINGS_COLLECTION].find_one(
        {"_id": "tracked_topics"}
    )
    if settings and settings.get("topics"):
        return tuple(str(topic) for topic in settings["topics"] if str(topic).strip())

    return tuple(normalize_topic_name(topic) for topic in EPRINT_TRACKED_TOPICS)


def set_tracked_topics(topics: list[str]) -> tuple[str, ...]:
    """Persist the tracked topics list."""
    normalized = sorted(
        {normalize_topic_name(topic) for topic in topics if normalize_topic_name(topic)}
    )
    MONGO[DBNAME][DISCUSSION_SETTINGS_COLLECTION].replace_one(
        {"_id": "tracked_topics"},
        {"_id": "tracked_topics", "topics": normalized},
        upsert=True,
    )
    return tuple(normalized)


def add_tracked_topic(topic: str) -> tuple[str, ...]:
    """Add a tracked topic and return the updated list."""
    normalized = normalize_topic_name(topic)
    if not normalized:
        return get_tracked_topics()

    return set_tracked_topics(list(get_tracked_topics()) + [normalized])


def remove_tracked_topic(topic: str) -> tuple[str, ...]:
    """Remove a tracked topic and return the updated list."""
    normalized = normalize_topic_name(topic)
    return set_tracked_topics(
        [existing for existing in get_tracked_topics() if existing != normalized]
    )


def topic_matches(
    topic: str,
    title: str,
    abstract: str,
    category: str | None = None,
    keywords: list[str] | None = None,
) -> bool:
    """Check whether a tracked topic matches the paper metadata."""
    normalized_topic = normalize_topic_name(topic)
    keyword_values = [normalize_topic_name(keyword) for keyword in (keywords or [])]
    text = " ".join(
        filter(None, (title, abstract, category or "", " ".join(keyword_values)))
    ).lower()

    patterns = TAG_PATTERNS.get(normalized_topic)
    if patterns and any(re.search(pattern, text) for pattern in patterns):
        return True

    if any(
        normalized_topic == keyword
        or normalized_topic in keyword
        or keyword in normalized_topic
        for keyword in keyword_values
    ):
        return True

    return normalized_topic in text


def derive_topic_tags(
    title: str,
    abstract: str,
    category: str | None = None,
    keywords: list[str] | None = None,
    tracked_topics: tuple[str, ...] | None = None,
) -> list[str]:
    """Derive tracked topics from the paper metadata."""
    topics = tracked_topics or get_tracked_topics()
    return [
        topic
        for topic in topics
        if topic_matches(
            topic, title=title, abstract=abstract, category=category, keywords=keywords
        )
    ]
