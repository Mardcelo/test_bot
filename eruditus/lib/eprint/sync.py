import logging
from datetime import datetime, timezone
from typing import Any

import discord

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
    paper: dict[str, Any], discussion: dict[str, Any] | None = None
) -> discord.Embed:
    """Build the feed embed for a paper discussion."""
    links = [f"[Paper]({paper['paper_url']})", f"[PDF]({paper['pdf_url']})"]
    if paper["git_links"]:
        links.extend(
            f"[Repo {index + 1}]({link})"
            for index, link in enumerate(paper["git_links"][:3])
        )

    description = truncate(paper["abstract"], 1200)
    if paper["withdrawn"]:
        description = f"**Withdrawn paper.**\n\n{description}"

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
    embed.add_field(
        name="Discussion",
        value=format_discussion_locations(discussion),
        inline=False,
    )
    embed.set_footer(text=f"Last modified: {paper['lastmodified']} UTC")
    return embed


async def ensure_discussion_feed_channel(guild: discord.Guild) -> discord.TextChannel:
    """Resolve or create the feed channel used for paper announcements."""
    if DISCUSSION_CHANNEL:
        channel = guild.get_channel(DISCUSSION_CHANNEL)
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


def ensure_discussion_forum_channel(
    guild: discord.Guild,
) -> discord.ForumChannel | None:
    """Resolve the optional forum channel used to mirror paper discussions."""
    if DISCUSSION_FORUM_CHANNEL is None:
        return None

    channel = guild.get_channel(DISCUSSION_FORUM_CHANNEL)
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


async def _prime_thread(
    thread: discord.Thread, paper: dict[str, Any], feed_channel: discord.TextChannel
) -> None:
    """Send the initial thread context message."""
    await thread.send(
        "\n".join(
            [
                f"Started discussion for **{paper['title']}**.",
                f"Feed channel: {feed_channel.mention}",
                f"Paper: {paper['paper_url']}",
                f"PDF: {paper['pdf_url']}",
            ]
        )
    )


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


async def ensure_discussion_for_paper(
    client: discord.Client,
    guild: discord.Guild,
    paper: dict[str, Any],
    feed_channel: discord.TextChannel | None = None,
) -> tuple[dict[str, Any] | None, bool]:
    """Create or repair the discussion objects for a paper."""
    if paper["withdrawn"]:
        discussion = MONGO[DBNAME][DISCUSSION_COLLECTION].find_one(
            {"_id": paper["_id"]}
        )
        if not discussion:
            return None, False
    feed_channel = feed_channel or await ensure_discussion_feed_channel(guild)
    forum_channel = ensure_discussion_forum_channel(guild)

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
        await _prime_thread(thread, paper, feed_channel)

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

    if message is None:
        message = await feed_channel.send(
            embed=build_discussion_embed(
                paper,
                discussion=text_discussion,
            ),
            view=DiscussionButton(
                paper_id=paper["_id"], disabled=paper["withdrawn"] or thread is None
            ),
        )
    else:
        await message.edit(
            embed=build_discussion_embed(
                paper,
                discussion=text_discussion,
            ),
            view=DiscussionButton(
                paper_id=paper["_id"], disabled=paper["withdrawn"] or thread is None
            ),
        )

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
