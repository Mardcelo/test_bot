import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import discord
from pymongo.errors import DuplicateKeyError

from config import (
    DBNAME,
    DISCUSSION_CHANNEL,
    DISCUSSION_COLLECTION,
    DISCUSSION_FORUM_CHANNEL,
    DISCUSSION_SETTINGS_COLLECTION,
    DISCUSSION_SUPPRESSION_COLLECTION,
    EPRINT_FEED_CHANNEL_NAME,
    MONGO,
    PAPER_COLLECTION,
)
from lib.eprint.scraper import fetch_recent_papers, parse_eprint_datetime
from lib.util import sanitize_channel_name, truncate
from msg_components.buttons.discussion import DiscussionButton

_log = logging.getLogger("discord.eruditus.eprint.sync")
DISCUSSION_LOCK_TIMEOUT = timedelta(minutes=10)
DISCUSSION_LOCK_WAIT_SECONDS = 30
DISCUSSION_LOCK_POLL_SECONDS = 0.5


def format_discussion_locations(discussion: dict[str, Any] | None = None) -> str:
    """Render the available discussion entry points for embeds and lists."""
    if discussion is None:
        return "Use the button below to join the private thread."

    locations = []
    if discussion.get("thread_id"):
        locations.append(f"Private thread: <#{discussion['thread_id']}>")
    if discussion.get("forum_thread_id"):
        locations.append(f"Forum post: <#{discussion['forum_thread_id']}>")

    return (
        "\n".join(locations)
        if locations
        else ("Use the button below to join the private thread.")
    )


def _discussion_lock_collection_name() -> str:
    """Return the collection name used for cross-process discussion locks."""
    return f"{DISCUSSION_COLLECTION}_lock"


def _parse_lock_expiry(value: Any) -> datetime | None:
    """Parse a stored lock-expiry timestamp."""
    if not value:
        return None

    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _try_acquire_discussion_lock(paper_id: str, token: str, now: datetime) -> bool:
    """Try to acquire the per-paper discussion lock."""
    collection = MONGO[DBNAME][_discussion_lock_collection_name()]
    lock_doc = {
        "_id": paper_id,
        "token": token,
        "expires_at": (now + DISCUSSION_LOCK_TIMEOUT).isoformat(),
    }

    try:
        collection.insert_one(lock_doc)
        return True
    except DuplicateKeyError:
        existing = collection.find_one({"_id": paper_id})
        if existing is None:
            return False

        expires_at = _parse_lock_expiry(existing.get("expires_at"))
        if expires_at is not None and expires_at > now:
            return False

        replaced = collection.replace_one(
            {
                "_id": paper_id,
                "token": existing.get("token"),
                "expires_at": existing.get("expires_at"),
            },
            lock_doc,
        )
        return replaced.modified_count == 1


async def acquire_discussion_lock(paper_id: str) -> str | None:
    """Wait briefly for the per-paper discussion lock."""
    token = uuid4().hex
    deadline = datetime.now(timezone.utc) + timedelta(
        seconds=DISCUSSION_LOCK_WAIT_SECONDS
    )

    while True:
        now = datetime.now(timezone.utc)
        if _try_acquire_discussion_lock(paper_id, token, now):
            return token

        if now >= deadline:
            return None

        await asyncio.sleep(DISCUSSION_LOCK_POLL_SECONDS)


def release_discussion_lock(paper_id: str, token: str) -> None:
    """Release the per-paper discussion lock held by this process."""
    MONGO[DBNAME][_discussion_lock_collection_name()].delete_one(
        {"_id": paper_id, "token": token}
    )


def format_topic_tags(paper: dict[str, Any]) -> str:
    """Render topic tags for embeds and list views."""
    tags = paper.get("iacr_tags") or paper.get("topic_tags") or ["untagged"]
    return ", ".join(f"`{tag}`" for tag in tags)


def discussion_auto_add_enabled() -> bool:
    """Check whether automatic discussion creation is enabled."""
    settings = MONGO[DBNAME][DISCUSSION_SETTINGS_COLLECTION].find_one(
        {"_id": "automation"}
    )
    if settings is None:
        return True

    return bool(settings.get("auto_add_enabled", True))


def set_discussion_auto_add_enabled(enabled: bool) -> None:
    """Persist the automatic discussion-creation toggle."""
    MONGO[DBNAME][DISCUSSION_SETTINGS_COLLECTION].replace_one(
        {"_id": "automation"},
        {"_id": "automation", "auto_add_enabled": enabled},
        upsert=True,
    )


def upsert_paper_record(
    paper: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Persist a paper document and return the stored document plus previous state."""
    existing_paper = MONGO[DBNAME][PAPER_COLLECTION].find_one({"_id": paper["_id"]})
    now = datetime.now(timezone.utc).isoformat()
    paper_doc = {
        **paper,
        "created_at": existing_paper["created_at"] if existing_paper else now,
        "updated_at": now,
    }
    MONGO[DBNAME][PAPER_COLLECTION].replace_one(
        {"_id": paper["_id"]}, paper_doc, upsert=True
    )
    return paper_doc, existing_paper


def build_thread_name(paper: dict[str, Any]) -> str:
    """Create a Discord-safe thread name for a paper discussion."""
    prefix = "🚫" if paper["withdrawn"] else "📄"
    slug = sanitize_channel_name(paper["title"])[:90].strip("-") or paper[
        "_id"
    ].replace("/", "-")
    return f"{prefix}-{slug}"[:100]


def build_discussion_embed(
    paper: dict[str, Any],
    discussion: dict[str, Any] | None = None,
    *,
    include_abstract: bool = True,
    include_discussion: bool = True,
) -> discord.Embed:
    """Build the feed embed for a paper discussion."""
    links = [f"[Paper]({paper['paper_url']})", f"[PDF]({paper['pdf_url']})"]
    if paper["git_links"]:
        links.extend(
            f"[Repo {index + 1}]({link})"
            for index, link in enumerate(paper["git_links"][:3])
        )

    description = None
    if include_abstract:
        description = truncate(paper["abstract"], 1200)
        if paper["withdrawn"]:
            description = f"**Withdrawn paper.**\n\n{description}"
    elif paper["withdrawn"]:
        description = "**Withdrawn paper.**"

    embed = discord.Embed(
        title=f"{paper['title']} ({paper['_id']})",
        url=paper["paper_url"],
        description=description,
        colour=discord.Colour.red() if paper["withdrawn"] else discord.Colour.blue(),
        timestamp=parse_eprint_datetime(paper["lastmodified"]),
    )
    embed.add_field(
        name="Authors",
        value=truncate(", ".join(paper["authors"]) or "Unknown", 1024),
        inline=False,
    )
    embed.add_field(name="Category", value=paper["category"], inline=True)
    embed.add_field(
        name="IACR Tags",
        value=format_topic_tags(paper),
        inline=True,
    )
    if paper.get("topic_tags"):
        embed.add_field(
            name="Tracked Topics",
            value=", ".join(f"`{tag}`" for tag in paper["topic_tags"]),
            inline=True,
        )
    embed.add_field(name="Links", value=" | ".join(links), inline=False)
    if include_discussion:
        embed.add_field(
            name="Discussion",
            value=format_discussion_locations(discussion),
            inline=False,
        )
    embed.set_footer(text=f"Last modified: {paper['lastmodified']} UTC")
    return embed


def build_thread_context_embed(
    paper: dict[str, Any], feed_channel: discord.TextChannel
) -> discord.Embed:
    """Build the private-thread context message with the paper abstract."""
    embed = build_discussion_embed(
        paper,
        include_abstract=True,
        include_discussion=False,
    )
    embed.add_field(name="Feed Channel", value=feed_channel.mention, inline=False)
    return embed


async def ensure_discussion_feed_channel(guild: discord.Guild) -> discord.TextChannel:
    """Resolve or create the feed channel used for paper announcements."""
    if DISCUSSION_CHANNEL:
        channel = await resolve_guild_channel(guild, DISCUSSION_CHANNEL)
        if isinstance(channel, discord.TextChannel):
            return channel
        raise RuntimeError(
            "DISCUSSION_CHANNEL is set but does not point to a text channel."
        )

    for channel in guild.text_channels:
        if channel.name == EPRINT_FEED_CHANNEL_NAME:
            return channel

    return await guild.create_text_channel(
        EPRINT_FEED_CHANNEL_NAME,
        reason="Create the ePrint discussion feed channel.",
        default_auto_archive_duration=10080,
    )


async def ensure_discussion_forum_channel(
    guild: discord.Guild,
) -> discord.ForumChannel | None:
    """Resolve the optional forum channel used to mirror paper discussions."""
    if DISCUSSION_FORUM_CHANNEL is None:
        return None

    channel = await resolve_guild_channel(guild, DISCUSSION_FORUM_CHANNEL)
    if isinstance(channel, discord.ForumChannel):
        return channel

    _log.warning(
        "DISCUSSION_FORUM_CHANNEL=%s is set but does not point to a forum channel.",
        DISCUSSION_FORUM_CHANNEL,
    )
    return None


async def resolve_thread(
    client: discord.Client, guild: discord.Guild, thread_id: int | None
) -> discord.Thread | None:
    """Fetch a discussion thread by ID, using cache first."""
    if not thread_id:
        return None

    thread = guild.get_thread(thread_id)
    if isinstance(thread, discord.Thread):
        return thread

    cached = client.get_channel(thread_id)
    if isinstance(cached, discord.Thread):
        return cached

    try:
        fetched = await client.fetch_channel(thread_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None

    return fetched if isinstance(fetched, discord.Thread) else None


async def resolve_guild_channel(
    guild: discord.Guild, channel_id: int | None
) -> discord.abc.GuildChannel | None:
    """Fetch a guild channel by ID, using cache first."""
    if not channel_id:
        return None

    channel = guild.get_channel(channel_id)
    if isinstance(channel, discord.abc.GuildChannel):
        return channel

    try:
        fetched = await guild.fetch_channel(channel_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None

    return fetched if isinstance(fetched, discord.abc.GuildChannel) else None


async def resolve_message(
    feed_channel: discord.TextChannel, message_id: int | None
) -> discord.Message | None:
    """Fetch the announcement message if it still exists."""
    if not message_id:
        return None

    try:
        return await feed_channel.fetch_message(message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None


async def resolve_thread_message(
    thread: discord.Thread, message_id: int | None
) -> discord.Message | None:
    """Fetch a thread message if it still exists."""
    if not message_id:
        return None

    try:
        return await thread.fetch_message(message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None


async def resolve_thread_context_message(
    thread: discord.Thread, message_id: int | None, bot_user_id: int | None
) -> discord.Message | None:
    """Fetch the thread context message, falling back to the first bot post."""
    message = await resolve_thread_message(thread, message_id)
    if message is not None or bot_user_id is None:
        return message

    try:
        async for candidate in thread.history(limit=10, oldest_first=True):
            if candidate.author.id == bot_user_id:
                return candidate
    except (discord.Forbidden, discord.HTTPException):
        return None

    return None


def _is_old_message_edit_limit(err: discord.HTTPException) -> bool:
    """Return True when Discord refuses further edits to an old message."""
    return err.code == 30046


async def upsert_thread_context_message(
    thread: discord.Thread,
    message: discord.Message | None,
    *,
    embed: discord.Embed,
) -> discord.Message:
    """Create or update the private-thread context message."""
    if message is None:
        return await thread.send(embed=embed)

    try:
        await message.edit(embed=embed)
        return message
    except discord.HTTPException as err:
        if _is_old_message_edit_limit(err):
            _log.warning(
                "Thread context message %s in %s hit Discord's old-message edit "
                "limit; keeping the existing intro message.",
                message.id,
                thread.id,
            )
            return message
        raise


def _forum_tag_name(name: str) -> str:
    """Normalize a topic tag into a Discord forum-tag label."""
    normalized = " ".join(str(name).split()).lower()
    return normalized[:20] or "untagged"


async def ensure_forum_tags(
    forum_channel: discord.ForumChannel,
    paper: dict[str, Any],
) -> list[discord.ForumTag]:
    """Resolve or create the forum tags needed for a paper mirror post."""
    requested_names = paper.get("topic_tags") or ["untagged"]
    known_tags = {tag.name.casefold(): tag for tag in forum_channel.available_tags}
    resolved = []

    for raw_name in requested_names:
        tag_name = _forum_tag_name(raw_name)
        if not tag_name or any(tag.name == tag_name for tag in resolved):
            continue

        tag = known_tags.get(tag_name.casefold())
        if tag is None:
            if len(known_tags) >= 20:
                _log.warning(
                    "Forum %s reached the 20-tag limit; skipping tag %r.",
                    forum_channel.id,
                    tag_name,
                )
                continue
            try:
                tag = await forum_channel.create_tag(name=tag_name)
            except (discord.Forbidden, discord.HTTPException) as err:
                _log.warning(
                    "Unable to create forum tag %r in %s: %s",
                    tag_name,
                    forum_channel.id,
                    err,
                )
                continue

            forum_channel._available_tags[tag.id] = tag
            known_tags[tag_name.casefold()] = tag

        resolved.append(tag)
        if len(resolved) == 5:
            break

    return resolved


async def upsert_announcement_message(
    feed_channel: discord.TextChannel,
    message: discord.Message | None,
    *,
    embed: discord.Embed,
    view: discord.ui.View,
) -> discord.Message:
    """Create or update the feed announcement, recreating it when edits are blocked."""
    if message is None:
        return await feed_channel.send(embed=embed, view=view)

    try:
        await message.edit(embed=embed, view=view)
        return message
    except discord.HTTPException as err:
        if not _is_old_message_edit_limit(err):
            raise

    _log.warning(
        "Announcement message %s hit Discord's old-message edit limit; recreating it.",
        message.id,
    )
    replacement = await feed_channel.send(embed=embed, view=view)
    try:
        await message.delete()
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        pass
    return replacement


async def ensure_discussion_for_paper(
    client: discord.Client,
    guild: discord.Guild,
    paper: dict[str, Any],
    feed_channel: discord.TextChannel | None = None,
) -> tuple[dict[str, Any] | None, bool]:
    """Create or repair the discussion objects for a paper."""
    lock_token = await acquire_discussion_lock(paper["_id"])
    if lock_token is None:
        _log.warning(
            "Timed out waiting for the discussion lock for %s; reusing stored state.",
            paper["_id"],
        )
        existing = MONGO[DBNAME][DISCUSSION_COLLECTION].find_one({"_id": paper["_id"]})
        return existing, False

    try:
        if paper["withdrawn"]:
            discussion = MONGO[DBNAME][DISCUSSION_COLLECTION].find_one(
                {"_id": paper["_id"]}
            )
            if not discussion:
                return None, False
        feed_channel = feed_channel or await ensure_discussion_feed_channel(guild)
        forum_channel = await ensure_discussion_forum_channel(guild)

        existing = MONGO[DBNAME][DISCUSSION_COLLECTION].find_one({"_id": paper["_id"]})
        now = datetime.now(timezone.utc).isoformat()
        thread = await resolve_thread(
            client, guild, existing["thread_id"] if existing else None
        )
        forum_thread = await resolve_thread(
            client, guild, existing.get("forum_thread_id") if existing else None
        )
        created = existing is None

        if thread is None and not paper["withdrawn"]:
            thread = await feed_channel.create_thread(
                name=build_thread_name(paper),
                invitable=False,
                auto_archive_duration=10080,
            )

        text_discussion = {
            "thread_id": thread.id if thread else None,
            "forum_thread_id": forum_thread.id if forum_thread else None,
        }

        if forum_channel is not None:
            forum_tags = await ensure_forum_tags(forum_channel, paper)
            forum_message = None
            if forum_thread is None and not paper["withdrawn"]:
                try:
                    forum_thread, forum_message = await forum_channel.create_thread(
                        name=build_thread_name(paper),
                        embed=build_discussion_embed(paper, discussion=text_discussion),
                        applied_tags=forum_tags,
                        view=DiscussionButton(
                            paper_id=paper["_id"],
                            disabled=paper["withdrawn"] or thread is None,
                        ),
                        auto_archive_duration=10080,
                        reason="Create the mirrored ePrint forum post.",
                    )
                    text_discussion["forum_thread_id"] = forum_thread.id
                except (discord.Forbidden, discord.HTTPException) as err:
                    _log.warning(
                        "Unable to create forum post for %s in %s: %s",
                        paper["_id"],
                        forum_channel.id,
                        err,
                    )
                    forum_thread = None
            elif forum_thread is not None:
                try:
                    await forum_thread.edit(
                        name=build_thread_name(paper),
                        applied_tags=forum_tags,
                    )
                except (discord.Forbidden, discord.HTTPException) as err:
                    _log.warning(
                        "Unable to update forum thread %s for %s: %s",
                        forum_thread.id,
                        paper["_id"],
                        err,
                    )

                forum_message = await resolve_thread_message(
                    forum_thread, existing.get("forum_message_id") if existing else None
                )
        else:
            forum_message = None

        if existing:
            message = await resolve_message(
                feed_channel, existing.get("announcement_message_id")
            )
        else:
            message = None

        message = await upsert_announcement_message(
            feed_channel,
            message,
            embed=build_discussion_embed(
                paper,
                discussion=text_discussion,
                include_abstract=False,
            ),
            view=DiscussionButton(
                paper_id=paper["_id"], disabled=paper["withdrawn"] or thread is None
            ),
        )

        if thread is not None:
            thread_context_message = await resolve_thread_context_message(
                thread,
                existing.get("thread_context_message_id") if existing else None,
                client.user.id if client.user else None,
            )
            thread_context_message = await upsert_thread_context_message(
                thread,
                thread_context_message,
                embed=build_thread_context_embed(paper, feed_channel),
            )
        else:
            thread_context_message = None

        if forum_thread is not None:
            forum_message = forum_message or await resolve_thread_message(
                forum_thread, existing.get("forum_message_id") if existing else None
            )
            if forum_message is not None:
                try:
                    await forum_message.edit(
                        embed=build_discussion_embed(
                            paper,
                            discussion={
                                "thread_id": thread.id if thread else None,
                                "forum_thread_id": forum_thread.id,
                            },
                        ),
                        view=DiscussionButton(
                            paper_id=paper["_id"],
                            disabled=paper["withdrawn"] or thread is None,
                        ),
                    )
                except (discord.Forbidden, discord.HTTPException) as err:
                    if _is_old_message_edit_limit(err):
                        _log.warning(
                            "Forum starter message %s for %s hit Discord's old-message "
                            "edit limit; keeping the existing starter message.",
                            forum_message.id,
                            paper["_id"],
                        )
                    else:
                        _log.warning(
                            "Unable to update forum starter message %s for %s: %s",
                            forum_message.id,
                            paper["_id"],
                            err,
                        )

        discussion = {
            "_id": paper["_id"],
            "paper_id": paper["_id"],
            "thread_id": thread.id if thread else None,
            "thread_context_message_id": (
                thread_context_message.id
                if thread_context_message
                else existing.get("thread_context_message_id")
                if existing
                else None
            ),
            "announcement_message_id": message.id,
            "feed_channel_id": feed_channel.id,
            "forum_thread_id": forum_thread.id if forum_thread else None,
            "forum_message_id": (
                forum_message.id
                if forum_message
                else existing.get("forum_message_id")
                if existing
                else None
            ),
            "forum_channel_id": (
                forum_channel.id
                if forum_channel
                else existing.get("forum_channel_id")
                if existing
                else None
            ),
            "member_ids": existing["member_ids"] if existing else [],
            "status": "withdrawn" if paper["withdrawn"] else "active",
            "created_at": existing["created_at"] if existing else now,
            "updated_at": now,
        }

        MONGO[DBNAME][DISCUSSION_COLLECTION].replace_one(
            {"_id": discussion["_id"]}, discussion, upsert=True
        )

        if thread is not None:
            await thread.edit(name=build_thread_name(paper))
            for member_id in discussion["member_ids"]:
                member = guild.get_member(member_id)
                if member is None:
                    try:
                        member = await guild.fetch_member(member_id)
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        continue
                await thread.add_user(member)

        return discussion, created
    finally:
        release_discussion_lock(paper["_id"], lock_token)


async def sync_recent_papers(
    client: discord.Client,
    guild: discord.Guild,
    days: int | None = None,
) -> dict[str, int]:
    """Fetch recent papers and synchronize discussions into Discord."""
    feed_channel = await ensure_discussion_feed_channel(guild)
    papers = await fetch_recent_papers(days=days)

    stats = {
        "matched": len(papers),
        "created": 0,
        "updated": 0,
        "withdrawn": 0,
        "skipped": 0,
    }

    for paper in papers:
        if MONGO[DBNAME][DISCUSSION_SUPPRESSION_COLLECTION].find_one(
            {"_id": paper["_id"]}
        ):
            stats["skipped"] += 1
            continue

        paper_doc, existing_paper = upsert_paper_record(paper)
        existing_discussion = MONGO[DBNAME][DISCUSSION_COLLECTION].find_one(
            {"_id": paper["_id"]}
        )

        if paper["withdrawn"] and existing_discussion is None:
            stats["skipped"] += 1
            continue

        discussion, created = await ensure_discussion_for_paper(
            client=client,
            guild=guild,
            paper=paper_doc,
            feed_channel=feed_channel,
        )
        if discussion is None:
            stats["skipped"] += 1
            continue

        if created:
            stats["created"] += 1
            continue

        if paper["withdrawn"]:
            stats["withdrawn"] += 1
            continue

        if (
            existing_paper is None
            or existing_paper.get("source_hash") != paper["source_hash"]
        ):
            stats["updated"] += 1

    _log.info("ePrint sync stats: %s", stats)
    return stats
