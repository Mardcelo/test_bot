import os
import random
from pathlib import Path
from typing import Callable, Iterable, Optional, TypeVar

import dotenv
from pymongo import MongoClient

T = TypeVar("T")
dotenv.load_dotenv()


class RandomUserAgent:
    """A class that represents a random User-Agent."""

    def __init__(self):
        self.user_agents = [
            line.strip()
            for line in open(
                f"{Path(__file__).parent}/user-agents.txt", encoding="utf-8"
            ).readlines()
        ]

    def __call__(self):
        return random.choice(self.user_agents)


def load_revision() -> str:
    """Get the current revision.

    Author:
        @es3n1n (refactoring and handling of multiple cases)

    Notes:
        We start by looking up the `.revision` file, if it's present, we use it.
        Otherwise, we try using the `.git` folder by reading `refs/heads/master`.
    """
    root_dir: Path = Path(__file__).parent
    dot_revision: Path = root_dir / ".revision"

    if dot_revision.exists():
        return open(dot_revision, encoding="utf-8").read()

    git_dir: Path = root_dir.parent / ".git"

    head_ref: Path = git_dir / "refs" / "heads" / "master"
    if head_ref.exists():
        return open(head_ref, encoding="utf-8").read()

    return "unknown"


def load_nullable_env_var(
    name: str, factory: Callable[[str], T] = lambda x: x, default: Optional[T] = None
) -> Optional[T]:
    """Load a nullable config var.
    Author:
        @es3n1n
    """
    var = os.getenv(name)
    return default if not var else factory(var)


def load_csv_env_var(name: str, default: Iterable[str]) -> tuple[str, ...]:
    """Load a comma-separated config variable."""
    var = os.getenv(name)
    if not var:
        return tuple(default)

    return tuple(part.strip() for part in var.split(",") if part.strip())


# fmt: off
# flake8: noqa
CHALLENGE_COLLECTION = os.getenv("CHALLENGE_COLLECTION")
CTF_COLLECTION = os.getenv("CTF_COLLECTION")
WORKON_COLLECTION = os.getenv("WORKON_COLLECTION")
PAPER_COLLECTION = os.getenv("PAPER_COLLECTION", "paper")
DISCUSSION_COLLECTION = os.getenv("DISCUSSION_COLLECTION", "discussion")
DISCUSSION_SUPPRESSION_COLLECTION = os.getenv(
    "DISCUSSION_SUPPRESSION_COLLECTION", "discussion_suppression"
)
DISCUSSION_SETTINGS_COLLECTION = os.getenv(
    "DISCUSSION_SETTINGS_COLLECTION", "discussion_settings"
)
CTFTIME_URL = os.getenv("CTFTIME_URL")
DATE_FORMAT = os.getenv("DATE_FORMAT")
DBNAME = os.getenv("DBNAME")
DEVELOPER_USER_ID = os.getenv("DEVELOPER_USER_ID")
GUILD_ID = int(os.getenv("GUILD_ID"))
MAX_CONTENT_SIZE = int(os.getenv("MAX_CONTENT_SIZE"))
MONGODB_URI = os.getenv("MONGODB_URI")
USER_AGENT = RandomUserAgent()
WRITEUP_INDEX_API = os.getenv("WRITEUP_INDEX_API")
TEAM_NAME = os.getenv("TEAM_NAME")
TEAM_EMAIL = os.getenv("TEAM_EMAIL")
MIN_PLAYERS = int(os.getenv("MIN_PLAYERS"))
COMMIT_HASH = load_revision()
BOOKMARK_CHANNEL = int(os.getenv("BOOKMARK_CHANNEL"))
REMINDER_CHANNEL = load_nullable_env_var("REMINDER_CHANNEL", factory=int)
DISCUSSION_CHANNEL = load_nullable_env_var("DISCUSSION_CHANNEL", factory=int)
CTFTIME_TEAM_ID = load_nullable_env_var("CTFTIME_TEAM_ID", factory=int)
CTFTIME_TRACKING_CHANNEL = load_nullable_env_var("CTFTIME_TRACKING_CHANNEL", factory=int)
CTFTIME_LEADERBOARD_CHANNEL = load_nullable_env_var("CTFTIME_LEADERBOARD_CHANNEL", factory=int)
NOTIFICATIONS_DISABLE_UNINTERESTED = load_nullable_env_var("NOTIFICATIONS_DISABLE_UNINTERESTED", factory=bool, default=False)
EPRINT_JSON_URL = os.getenv(
    "EPRINT_JSON_URL", "https://eprint.iacr.org/rss/rss.xml?order=recent"
)
EPRINT_FEED_CHANNEL_NAME = os.getenv("EPRINT_FEED_CHANNEL_NAME", "papers-feed")
EPRINT_CACHE_PATH = os.getenv("EPRINT_CACHE_PATH", "data/eprint/recent_papers.json")
EPRINT_LOOKBACK_DAYS = int(os.getenv("EPRINT_LOOKBACK_DAYS", "30"))
EPRINT_SYNC_MINUTES = max(1, int(os.getenv("EPRINT_SYNC_MINUTES", "180")))
EPRINT_TRACKED_TOPICS = load_csv_env_var(
    "EPRINT_TRACKED_TOPICS", default=("isogeny", "pqc", "lattice")
)

MONGO = MongoClient(MONGODB_URI)
