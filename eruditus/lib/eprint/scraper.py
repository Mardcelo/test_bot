import json
import logging
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from hashlib import md5
from pathlib import Path
from typing import Any, AsyncIterator
from xml.etree import ElementTree as ET

import aiohttp
from bs4 import BeautifulSoup

try:
    from scrapling.fetchers import FetcherSession
except ImportError:  # pragma: no cover - exercised only before dependencies install.
    FetcherSession = None
else:
    logging.getLogger("scrapling").setLevel(logging.WARNING)

from config import EPRINT_CACHE_PATH, EPRINT_JSON_URL, EPRINT_LOOKBACK_DAYS
from lib.eprint.tagger import derive_topic_tags, get_tracked_topics

_log = logging.getLogger("discord.eruditus.eprint.scraper")
DC_NAMESPACE = {"dc": "http://purl.org/dc/elements/1.1/"}
PAPER_ID_RE = re.compile(r"(\d{4}/\d+)")
CHARSET_RE = re.compile(r"charset=([^\s;]+)", re.IGNORECASE)
EPRINT_FALLBACK_HEADERS = {
    "Accept": "application/rss+xml, application/xml, text/html;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}
EPRINT_FETCHER_KIND = tuple[str, Any]


def parse_eprint_datetime(value: str) -> datetime:
    """Parse an ePrint datetime string into an aware UTC datetime."""
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _parse_rss_datetime(value: str) -> datetime:
    """Parse an RSS pubDate into an aware UTC datetime."""
    return datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %z").astimezone(timezone.utc)


def _compact_whitespace(value: str | None) -> str:
    if not value:
        return ""

    return " ".join(value.replace("\r", " ").replace("\n", " ").split())


def _decode_response_body(body: bytes | str, headers: dict[str, Any]) -> str:
    """Decode a response body using its declared charset when present."""
    if isinstance(body, str):
        return body

    content_type = str(headers.get("content-type") or headers.get("Content-Type") or "")
    match = CHARSET_RE.search(content_type)
    charset = match.group(1).strip("\"'") if match else "utf-8"
    return body.decode(charset, errors="replace")


@asynccontextmanager
async def _open_scrapling_fetcher() -> AsyncIterator[EPRINT_FETCHER_KIND]:
    """Open a Scrapling HTTP session for ePrint requests."""
    if FetcherSession is None:
        raise aiohttp.ClientError("Scrapling is not installed")

    async with FetcherSession(
        impersonate="chrome",
        stealthy_headers=True,
        timeout=60,
        retries=3,
        follow_redirects="safe",
    ) as session:
        yield ("scrapling", session)


@asynccontextmanager
async def _open_aiohttp_fetcher() -> AsyncIterator[EPRINT_FETCHER_KIND]:
    """Open the legacy aiohttp session used as a fallback fetcher."""
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(
        timeout=timeout,
        headers=EPRINT_FALLBACK_HEADERS,
    ) as session:
        yield ("aiohttp", session)


async def _fetch_text(
    fetcher: EPRINT_FETCHER_KIND,
    url: str,
    *,
    allow_404: bool = False,
) -> str | None:
    """Fetch a URL through the active ePrint fetcher."""
    kind, session = fetcher

    if kind == "scrapling":
        try:
            page = await session.get(url)
        except Exception as err:
            raise aiohttp.ClientError(
                f"Scrapling request failed for {url}: {err}"
            ) from err

        status = int(getattr(page, "status", 0) or 0)
        if status == 404 and allow_404:
            return None
        if status >= 400:
            raise aiohttp.ClientError(f"ePrint returned HTTP {status} for {url}")

        return _decode_response_body(page.body, page.headers)

    async with session.get(url) as response:
        if response.status == 404 and allow_404:
            return None

        response.raise_for_status()
        return await response.text()


def _normalize_authors(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(author).strip() for author in value if str(author).strip()]

    if isinstance(value, str) and value.strip():
        return [value.strip()]

    return []


def _build_source_hash(paper: dict[str, Any]) -> str:
    fields = {
        key: paper[key]
        for key in (
            "_id",
            "title",
            "authors",
            "abstract",
            "category",
            "lastmodified",
            "withdrawn",
            "topic_tags",
            "iacr_tags",
            "paper_url",
            "pdf_url",
            "git_links",
        )
    }
    return md5(json.dumps(fields, sort_keys=True).encode("utf-8")).hexdigest()


def normalize_eprint_id(value: str) -> str | None:
    """Extract an ePrint ID from a raw command argument or URL."""
    match = PAPER_ID_RE.search(value.strip())
    return match.group(1) if match is not None else None


def _metadata_entries(soup: BeautifulSoup) -> dict[str, list[str]]:
    """Extract metadata definition list entries from an ePrint paper page."""
    metadata: dict[str, list[str]] = {}
    for term in soup.select("#metadata dt"):
        key = term.get_text(" ", strip=True).lower()
        values = []
        sibling = term.find_next_sibling()
        while sibling is not None and sibling.name == "dd":
            value = sibling.get_text(" ", strip=True)
            if value:
                values.append(value)
            sibling = sibling.find_next_sibling()

        metadata[key] = values

    return metadata


def _extract_repo_links(soup: BeautifulSoup) -> list[str]:
    """Extract repository links from the paper page."""
    repo_links: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if not href.startswith(("http://", "https://")):
            continue
        if "github.com/" not in href and "gitlab.com/" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        repo_links.append(href)

    return repo_links


def _parse_page_timestamp(value: str | None) -> str:
    """Normalize an ISO page timestamp to the bot's ePrint datetime format."""
    if not value:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return (
        datetime.fromisoformat(value)
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


def parse_paper_page(html_text: str, eprint_id: str) -> dict[str, Any] | None:
    """Parse a single ePrint paper page into a raw paper record."""
    soup = BeautifulSoup(html_text, "html.parser")
    metadata = _metadata_entries(soup)
    title_node = soup.select_one("h3.mb-3")
    og_title = soup.find("meta", attrs={"property": "og:title"})
    og_description = soup.find("meta", attrs={"property": "og:description"})
    modified_time = soup.find("meta", attrs={"property": "article:modified_time"})
    pdf_meta = soup.find("meta", attrs={"name": "citation_pdf_url"})
    category_values = metadata.get("category", [])

    title = (
        (og_title.get("content") if og_title else None)
        or (title_node.get_text(" ", strip=True) if title_node else "")
    ).strip()
    if not title:
        return None

    authors = [
        meta["content"].strip()
        for meta in soup.find_all("meta", attrs={"name": "citation_author"})
        if meta.get("content", "").strip()
    ]
    if not authors:
        authors = [
            author.get_text(" ", strip=True)
            for author in soup.select(".author .authorName")
            if author.get_text(" ", strip=True)
        ]

    category = category_values[0] if category_values else "Uncategorized"
    keywords = [
        keyword.get_text(" ", strip=True)
        for keyword in soup.select("#metadata dd.keywords a")
        if keyword.get_text(" ", strip=True)
    ]
    if not keywords:
        keywords = metadata.get("keywords", [])

    return {
        "name": eprint_id,
        "title": title,
        "abstract": (og_description.get("content") if og_description else "").strip(),
        "category": category,
        "authors": authors,
        "paper_url": f"https://eprint.iacr.org/{eprint_id}",
        "pdf_url": (
            pdf_meta.get("content").strip()
            if pdf_meta and pdf_meta.get("content")
            else f"https://eprint.iacr.org/{eprint_id}.pdf"
        ),
        "lastmodified": _parse_page_timestamp(
            modified_time.get("content") if modified_time else None
        ),
        "year": int(eprint_id.split("/")[0]),
        "pid": int(eprint_id.split("/")[1]),
        "withdrawn": False,
        "gits": _extract_repo_links(soup),
        "keywords": keywords,
    }


def normalize_paper(
    raw_paper: dict[str, Any],
    require_tracked_topics: bool = True,
    fallback_tags: list[str] | None = None,
) -> dict[str, Any] | None:
    """Normalize a raw ePrint record into the bot's paper schema."""
    link = str(raw_paper.get("paper_url") or raw_paper.get("link") or "").strip()
    name = str(raw_paper.get("name") or "").strip()
    if not name and link:
        name = link.rstrip("/").rsplit("/", maxsplit=1)[-1]
    if not name:
        year = raw_paper.get("year")
        pid = raw_paper.get("pid")
        if year is None or pid is None:
            return None
        name = f"{year}/{pid}"

    title = _compact_whitespace(raw_paper.get("title"))
    abstract = _compact_whitespace(
        raw_paper.get("abstract") or raw_paper.get("description")
    )
    category = _compact_whitespace(raw_paper.get("category")) or "Uncategorized"
    authors = _normalize_authors(raw_paper.get("authors"))
    pdf_url = str(raw_paper.get("pdf_url") or "").strip()
    if not pdf_url:
        pdffile = str(raw_paper.get("pdffile") or f"{name}.pdf").lstrip("/")
        pdf_url = f"https://eprint.iacr.org/{pdffile}"
    if not link:
        link = f"https://eprint.iacr.org/{name}"
    keywords = [
        str(keyword).strip()
        for keyword in raw_paper.get("keywords", [])
        if str(keyword).strip()
    ]
    tracked_topics = get_tracked_topics()
    iacr_tags = keywords or ([] if category == "Uncategorized" else [category])
    topic_tags = sorted(
        set(
            derive_topic_tags(
                title=title,
                abstract=abstract,
                category=category,
                keywords=keywords,
                tracked_topics=tracked_topics,
            )
        )
    )
    if not topic_tags:
        if require_tracked_topics:
            return None
        topic_tags = list(fallback_tags or [])

    paper = {
        "_id": name,
        "pid": int(raw_paper.get("pid", name.split("/")[-1])),
        "name": name,
        "year": int(raw_paper.get("year", name.split("/")[0])),
        "title": title,
        "authors": authors,
        "abstract": abstract,
        "category": category,
        "topic_tags": topic_tags,
        "iacr_tags": iacr_tags,
        "paper_url": link,
        "pdf_url": pdf_url,
        "git_links": list(raw_paper.get("gits", [])),
        "lastmodified": str(raw_paper.get("lastmodified")),
        "withdrawn": bool(raw_paper.get("withdrawn")),
    }
    paper["source_hash"] = _build_source_hash(paper)
    return paper


def parse_rss_feed(xml_text: str) -> list[dict[str, Any]]:
    """Parse the official ePrint RSS feed into raw paper records."""
    root = ET.fromstring(xml_text)
    papers = []
    for item in root.findall("./channel/item"):
        link = item.findtext("link", default="").strip()
        title = item.findtext("title", default="").strip()
        description = item.findtext("description", default="")
        category = item.findtext("category", default="").strip()
        pub_date = item.findtext("pubDate", default="").strip()
        enclosure = item.find("enclosure")
        authors = [
            creator.text.strip()
            for creator in item.findall("dc:creator", DC_NAMESPACE)
            if creator.text and creator.text.strip()
        ]
        if not link or not title or not pub_date:
            continue

        published_at = _parse_rss_datetime(pub_date)
        match = PAPER_ID_RE.search(link)
        if match is None:
            continue

        name = match.group(1)
        papers.append(
            {
                "name": name,
                "title": title,
                "description": description,
                "category": category,
                "authors": authors,
                "paper_url": link,
                "pdf_url": enclosure.get("url") if enclosure is not None else "",
                "lastmodified": published_at.strftime("%Y-%m-%d %H:%M:%S"),
                "year": int(name.split("/")[0]),
                "pid": int(name.split("/")[1]),
                "withdrawn": False,
                "gits": [],
            }
        )

    return papers


def write_snapshot(papers: list[dict[str, Any]], lookback_days: int) -> None:
    """Persist the current recent-paper snapshot to JSON."""
    cache_path = Path(EPRINT_CACHE_PATH)
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as err:
        _log.warning(
            "Unable to prepare ePrint cache directory %s: %s", cache_path.parent, err
        )
        return

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": lookback_days,
        "tracked_topics": list(get_tracked_topics()),
        "papers": papers,
    }
    try:
        cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as err:
        _log.warning("Unable to write ePrint cache snapshot %s: %s", cache_path, err)


async def fetch_recent_papers(days: int | None = None) -> list[dict[str, Any]]:
    """Fetch recent tracked papers from the official ePrint JSON feed."""
    lookback_days = max(1, min(days or EPRINT_LOOKBACK_DAYS, EPRINT_LOOKBACK_DAYS))
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    try:
        async with _open_scrapling_fetcher() as fetcher:
            papers = await _fetch_recent_papers_with_fetcher(fetcher, cutoff)
    except aiohttp.ClientError as err:
        _log.warning("Scrapling ePrint fetch failed; falling back to aiohttp: %s", err)
        async with _open_aiohttp_fetcher() as fetcher:
            papers = await _fetch_recent_papers_with_fetcher(fetcher, cutoff)

    papers.sort(key=lambda paper: paper["lastmodified"], reverse=True)
    write_snapshot(papers, lookback_days=lookback_days)
    return papers


async def _fetch_recent_papers_with_fetcher(
    fetcher: EPRINT_FETCHER_KIND,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    """Fetch recent papers with the provided ePrint fetcher."""
    payload = await _fetch_text(fetcher, EPRINT_JSON_URL)
    if payload is None:
        return []

    papers = []
    for raw_paper in parse_rss_feed(payload):
        normalized = normalize_paper(raw_paper)
        if normalized is None:
            continue

        try:
            lastmodified = parse_eprint_datetime(normalized["lastmodified"])
        except ValueError:
            _log.warning("Skipping paper with invalid timestamp: %s", normalized["_id"])
            continue

        if lastmodified < cutoff:
            continue

        try:
            page_paper = await _fetch_paper_by_id_with_fetcher(
                normalized["_id"],
                fetcher,
                require_tracked_topics=False,
            )
        except aiohttp.ClientError as err:
            _log.warning("Failed to fetch ePrint page %s: %s", normalized["_id"], err)
            page_paper = None
        if page_paper is not None:
            if not page_paper.get("topic_tags"):
                page_paper["topic_tags"] = normalized["topic_tags"]
                page_paper["source_hash"] = _build_source_hash(page_paper)

            normalized = page_paper

        papers.append(normalized)

    return papers


async def fetch_paper_by_id(
    identifier: str, session: aiohttp.ClientSession | None = None
) -> dict[str, Any] | None:
    """Fetch and normalize a single ePrint paper by ID or URL."""
    eprint_id = normalize_eprint_id(identifier)
    if eprint_id is None:
        return None

    if session is None:
        try:
            async with _open_scrapling_fetcher() as fetcher:
                return await _fetch_paper_by_id_with_fetcher(
                    eprint_id,
                    fetcher,
                    require_tracked_topics=False,
                )
        except aiohttp.ClientError as err:
            _log.warning(
                "Scrapling ePrint paper fetch failed; falling back to aiohttp: %s",
                err,
            )
            async with _open_aiohttp_fetcher() as fetcher:
                return await _fetch_paper_by_id_with_fetcher(
                    eprint_id,
                    fetcher,
                    require_tracked_topics=False,
                )

    return await _fetch_paper_by_id_with_fetcher(
        eprint_id,
        ("aiohttp", session),
        require_tracked_topics=False,
    )


async def _fetch_paper_by_id_with_fetcher(
    eprint_id: str,
    fetcher: EPRINT_FETCHER_KIND,
    *,
    require_tracked_topics: bool,
) -> dict[str, Any] | None:
    """Fetch and normalize a single ePrint paper using an open fetcher."""
    html = await _fetch_text(
        fetcher,
        f"https://eprint.iacr.org/{eprint_id}",
        allow_404=True,
    )
    if html is None:
        return None

    raw_paper = parse_paper_page(html, eprint_id)
    if raw_paper is None:
        return None

    return normalize_paper(
        raw_paper,
        require_tracked_topics=require_tracked_topics,
    )
