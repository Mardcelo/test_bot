import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
import discord
from discord import app_commands
from discord.app_commands import Choice

from config import (
    DBNAME,
    DISCUSSION_COLLECTION,
    DISCUSSION_SUPPRESSION_COLLECTION,
    MONGO,
    PAPER_COLLECTION,
)
from lib.discord_util import (
    add_discussion_member,
    is_deferred,
    remove_discussion_member,
)
from lib.eprint.scraper import normalize_eprint_id
from lib.eprint.sync import (
    discussion_auto_add_enabled,
    ensure_discussion_feed_channel,
    ensure_discussion_for_paper,
    format_topic_tags,
    resolve_message,
    resolve_thread,
    set_discussion_auto_add_enabled,
    sync_recent_papers,
)
from lib.eprint.tagger import (
    add_tracked_topic,
    get_tracked_topics,
    normalize_topic_name,
    remove_tracked_topic,
)
from lib.util import get_discussion_info, get_paper_info, truncate
from msg_components.buttons.discussion import LeaveDiscussionButton

_log = logging.getLogger("discord.eruditus.cmds.discussion")


class Discussion(app_commands.Group):
    """Manage ePrint paper discussions."""

    def __init__(self) -> None:
        super().__init__(
            name="discussion",
            description="Manage ePrint paper discussions.",
        )

    async def on_error(
        self, interaction: discord.Interaction, error: app_commands.AppCommandError
    ) -> None:
        _log.exception(
            "Exception occurred due to `/%s %s`",
            interaction.command.parent.name,
            interaction.command.name,
            exc_info=error,
        )
        msg = {"content": "An exception has occurred", "ephemeral": True}
        if is_deferred(interaction):
            await interaction.followup.send(**msg)
        elif interaction.response.type is None:
            await interaction.response.send_message(**msg)

    async def _paper_autocomplete(
        self, _: discord.Interaction, current: str
    ) -> list[Choice[str]]:
        query: dict[str, Any] = {}
        if current.strip():
            regex = re.compile(re.escape(current.strip()), re.IGNORECASE)
            query = {"$or": [{"title": regex}, {"_id": regex}]}

        cursor = (
            MONGO[DBNAME][PAPER_COLLECTION]
            .find(query)
            .sort("lastmodified", -1)
            .limit(25)
        )
        suggestions = []
        for paper in cursor:
            tags = ",".join(
                paper.get("iacr_tags") or paper.get("topic_tags") or ["untagged"]
            )
            label = truncate(f"{paper['title']} ({paper['_id']}) [{tags}]", 100)
            suggestions.append(Choice(name=label, value=paper["_id"]))

        return suggestions

    async def _tracked_topic_autocomplete(
        self, _: discord.Interaction, current: str
    ) -> list[Choice[str]]:
        current_normalized = normalize_topic_name(current)
        topics = [
            topic
            for topic in get_tracked_topics()
            if not current_normalized or current_normalized in topic
        ]
        return [Choice(name=truncate(topic, 100), value=topic) for topic in topics[:25]]

    async def _suppressed_paper_autocomplete(
        self, _: discord.Interaction, current: str
    ) -> list[Choice[str]]:
        query: dict[str, Any] = {}
        if current.strip():
            regex = re.compile(re.escape(current.strip()), re.IGNORECASE)
            query = {"_id": regex}

        cursor = (
            MONGO[DBNAME][DISCUSSION_SUPPRESSION_COLLECTION]
            .find(query)
            .sort("suppressed_at", -1)
            .limit(25)
        )
        return [Choice(name=entry["_id"], value=entry["_id"]) for entry in cursor]

    @app_commands.checks.bot_has_permissions(manage_channels=True)
    @app_commands.checks.has_permissions(manage_channels=True)
    @app_commands.command()
    async def sync(
        self, interaction: discord.Interaction, days: Optional[int] = None
    ) -> None:
        """Fetch recent ePrint papers from the last 30 days and sync discussions."""
        if not discussion_auto_add_enabled():
            await interaction.response.send_message(
                (
                    "Automatic discussion creation is currently stopped. "
                    "Use `/discussion start` to resume it."
                ),
                ephemeral=True,
            )
            return

        await interaction.response.defer()

        try:
            stats = await sync_recent_papers(
                interaction.client,
                interaction.guild,
                days=days,
            )
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            _log.warning("Failed to sync ePrint papers: %s", error)
            await interaction.followup.send(
                (
                    "Failed to fetch recent ePrint papers. Check the network "
                    "and try again."
                ),
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            (
                "ePrint sync finished.\n"
                f"Matched papers: {stats['matched']}\n"
                f"Created discussions: {stats['created']}\n"
                f"Updated discussions: {stats['updated']}\n"
                f"Withdrawn updates: {stats['withdrawn']}\n"
                f"Skipped: {stats['skipped']}"
            )
        )

    @app_commands.command(name="list")
    @app_commands.autocomplete(tag=_tracked_topic_autocomplete)  # type: ignore
    async def list_discussions(
        self,
        interaction: discord.Interaction,
        tag: Optional[str] = None,
        limit: Optional[int] = 10,
    ) -> None:
        """List recent paper discussions."""
        limit = 10 if limit is None else max(1, min(limit, 15))

        query: dict[str, Any] = {}
        if tag:
            query["topic_tags"] = normalize_topic_name(tag)

        papers = list(
            MONGO[DBNAME][PAPER_COLLECTION]
            .find(query)
            .sort("lastmodified", -1)
            .limit(limit)
        )
        if not papers:
            await interaction.response.send_message(
                "No papers found. Run `/discussion sync` first.", ephemeral=True
            )
            return

        lines = []
        for paper in papers:
            discussion = get_discussion_info(_id=paper["_id"])
            thread_value = (
                f"<#{discussion['thread_id']}>"
                if discussion and discussion.get("thread_id")
                else "pending"
            )
            lines.append(
                (
                    f"**{paper['_id']}** {format_topic_tags(paper)} "
                    f"{'withdrawn' if paper['withdrawn'] else 'active'}\n"
                    f"{truncate(paper['title'], 120)}\n"
                    f"{thread_value} | {paper['paper_url']}"
                )
            )

        embed = discord.Embed(
            title="Recent ePrint discussions",
            description="\n\n".join(lines),
            colour=discord.Colour.blurple(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.checks.bot_has_permissions(manage_channels=True)
    @app_commands.command()
    @app_commands.autocomplete(paper=_paper_autocomplete)  # type: ignore
    async def join(self, interaction: discord.Interaction, paper: str) -> None:
        """Join the private discussion thread for a paper."""
        paper_doc = get_paper_info(_id=paper)
        if paper_doc is None:
            await interaction.response.send_message("No such paper.", ephemeral=True)
            return

        if paper_doc["withdrawn"]:
            await interaction.response.send_message(
                "This paper is marked as withdrawn.", ephemeral=True
            )
            return

        discussion = get_discussion_info(_id=paper)
        if discussion is None or discussion.get("thread_id") is None:
            discussion, _ = await ensure_discussion_for_paper(
                interaction.client, interaction.guild, paper_doc
            )
            if discussion is None:
                await interaction.response.send_message(
                    (
                        "The discussion is not available yet. "
                        "Run `/discussion sync` first."
                    ),
                    ephemeral=True,
                )
                return

        if interaction.user.id in discussion["member_ids"]:
            await interaction.response.send_message(
                "You already joined this discussion.", ephemeral=True
            )
            return

        thread = await resolve_thread(
            interaction.client, interaction.guild, discussion["thread_id"]
        )
        if thread is None:
            await interaction.response.send_message(
                (
                    "The discussion thread is missing. "
                    "Run `/discussion sync` to repair it."
                ),
                ephemeral=True,
            )
            return

        await add_discussion_member(thread, discussion, interaction.user)
        await interaction.response.send_message(
            f"Added you to the discussion for `{paper_doc['title']}`.",
            view=LeaveDiscussionButton(paper_id=paper),
            ephemeral=True,
        )
        await thread.send(f"{interaction.user.mention} joined the discussion.")

    @app_commands.checks.bot_has_permissions(manage_channels=True)
    @app_commands.command()
    @app_commands.autocomplete(paper=_paper_autocomplete)  # type: ignore
    async def leave(
        self, interaction: discord.Interaction, paper: Optional[str] = None
    ) -> None:
        """Leave a paper discussion thread."""
        if paper is None:
            discussion = get_discussion_info(thread_id=interaction.channel_id)
            if discussion is None:
                await interaction.response.send_message(
                    "Run this command from within a discussion thread or pick a paper.",
                    ephemeral=True,
                )
                return
            paper = discussion["_id"]
        else:
            discussion = get_discussion_info(_id=paper)
            if discussion is None:
                await interaction.response.send_message(
                    "No such discussion.", ephemeral=True
                )
                return

        paper_doc = get_paper_info(_id=paper)
        if paper_doc is None:
            await interaction.response.send_message("No such paper.", ephemeral=True)
            return

        if interaction.user.id not in discussion["member_ids"]:
            await interaction.response.send_message(
                "You are not in this discussion.", ephemeral=True
            )
            return

        thread = await resolve_thread(
            interaction.client, interaction.guild, discussion["thread_id"]
        )
        if thread is None:
            member_ids = [
                member_id
                for member_id in discussion["member_ids"]
                if member_id != interaction.user.id
            ]
            MONGO[DBNAME][DISCUSSION_COLLECTION].update_one(
                {"_id": discussion["_id"]},
                {"$set": {"member_ids": member_ids}},
            )
            await interaction.response.send_message(
                "Removed you from the discussion state, but the thread is missing.",
                ephemeral=True,
            )
            return

        await remove_discussion_member(thread, discussion, interaction.user)
        await thread.send(f"{interaction.user.mention} left the discussion.")
        await interaction.response.send_message(
            f"Removed you from the discussion for `{paper_doc['title']}`.",
            ephemeral=True,
        )

    @app_commands.checks.has_permissions(manage_channels=True)
    @app_commands.command()
    async def stop(self, interaction: discord.Interaction) -> None:
        """Stop automatic creation of new discussions from the ePrint feed."""
        set_discussion_auto_add_enabled(False)
        await interaction.response.send_message(
            (
                "Automatic discussion creation is now stopped. "
                "`/create <eprint_id>` still works."
            ),
            ephemeral=True,
        )

    @app_commands.checks.has_permissions(manage_channels=True)
    @app_commands.command()
    async def start(self, interaction: discord.Interaction) -> None:
        """Resume automatic creation of new discussions from the ePrint feed."""
        set_discussion_auto_add_enabled(True)
        await interaction.response.send_message(
            "Automatic discussion creation is enabled again.",
            ephemeral=True,
        )

    @app_commands.command()
    async def topics(self, interaction: discord.Interaction) -> None:
        """Show the currently tracked topics."""
        topics = list(get_tracked_topics())
        await interaction.response.send_message(
            "Tracked topics:\n" + "\n".join(f"- `{topic}`" for topic in topics),
            ephemeral=True,
        )

    @app_commands.checks.has_permissions(manage_channels=True)
    @app_commands.command()
    async def addtopic(self, interaction: discord.Interaction, topic: str) -> None:
        """Add a tracked topic for future auto-created discussions."""
        updated_topics = add_tracked_topic(topic)
        normalized_topic = normalize_topic_name(topic)
        topics_summary = ", ".join(f"`{item}`" for item in updated_topics)
        await interaction.response.send_message(
            (
                f"Added tracked topic `{normalized_topic}`.\n"
                f"Tracked topics now: {topics_summary}\n"
                "Run `/discussion sync` to pick up recent matches."
            ),
            ephemeral=True,
        )

    @app_commands.checks.has_permissions(manage_channels=True)
    @app_commands.command()
    @app_commands.autocomplete(topic=_tracked_topic_autocomplete)  # type: ignore
    async def removetopic(self, interaction: discord.Interaction, topic: str) -> None:
        """Remove a tracked topic from future auto-created discussions."""
        normalized_topic = normalize_topic_name(topic)
        current_topics = set(get_tracked_topics())
        if normalized_topic not in current_topics:
            await interaction.response.send_message(
                f"`{normalized_topic}` is not currently tracked.",
                ephemeral=True,
            )
            return

        updated_topics = remove_tracked_topic(normalized_topic)
        topics_summary = ", ".join(f"`{item}`" for item in updated_topics)
        await interaction.response.send_message(
            (
                f"Removed tracked topic `{normalized_topic}`.\n"
                f"Tracked topics now: {topics_summary}"
            ),
            ephemeral=True,
        )

    @app_commands.checks.has_permissions(manage_channels=True)
    @app_commands.command()
    @app_commands.autocomplete(paper=_suppressed_paper_autocomplete)  # type: ignore
    async def unsuppress(
        self, interaction: discord.Interaction, paper: Optional[str] = None
    ) -> None:
        """Remove suppression for one paper or for all purged papers."""
        if paper is None:
            result = MONGO[DBNAME][DISCUSSION_SUPPRESSION_COLLECTION].delete_many({})
            await interaction.response.send_message(
                (
                    f"Cleared suppression for {result.deleted_count} paper(s). "
                    "Run `/discussion sync` to recreate matching discussions."
                ),
                ephemeral=True,
            )
            return

        normalized_id = normalize_eprint_id(paper)
        if normalized_id is None:
            await interaction.response.send_message(
                "Use an ePrint paper ID like `2026/666`.", ephemeral=True
            )
            return

        result = MONGO[DBNAME][DISCUSSION_SUPPRESSION_COLLECTION].delete_one(
            {"_id": normalized_id}
        )
        if result.deleted_count == 0:
            await interaction.response.send_message(
                f"`{normalized_id}` is not currently suppressed.", ephemeral=True
            )
            return

        await interaction.response.send_message(
            (
                f"Unsuppressed `{normalized_id}`. "
                "Run `/discussion sync` or `/create <eprint_id>` to recreate it."
            ),
            ephemeral=True,
        )

    @app_commands.checks.bot_has_permissions(manage_channels=True)
    @app_commands.checks.has_permissions(manage_channels=True)
    @app_commands.command()
    async def purge(
        self, interaction: discord.Interaction, count: Optional[int] = None
    ) -> None:
        """Delete recent discussion threads and suppress them from auto-sync."""
        if count is not None and count < 1:
            await interaction.response.send_message(
                "Provide a positive count, or omit it to purge all discussions.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        cursor = MONGO[DBNAME][DISCUSSION_COLLECTION].find().sort("updated_at", -1)
        discussions = list(cursor if count is None else cursor.limit(count))
        if not discussions:
            await interaction.followup.send("No discussions to purge.", ephemeral=True)
            return

        purged = 0
        failures: list[str] = []
        now = datetime.now(timezone.utc).isoformat()
        fallback_feed_channel = await ensure_discussion_feed_channel(interaction.guild)

        for discussion in discussions:
            paper_id = discussion["_id"]
            thread = await resolve_thread(
                interaction.client, interaction.guild, discussion.get("thread_id")
            )

            feed_channel = None
            feed_channel_id = discussion.get("feed_channel_id")
            if feed_channel_id:
                channel = interaction.guild.get_channel(feed_channel_id)
                if isinstance(channel, discord.TextChannel):
                    feed_channel = channel
            if feed_channel is None:
                feed_channel = fallback_feed_channel

            message = await resolve_message(
                feed_channel, discussion.get("announcement_message_id")
            )

            if thread is not None:
                try:
                    await thread.delete()
                except discord.NotFound:
                    pass
                except (discord.Forbidden, discord.HTTPException) as err:
                    failures.append(f"{paper_id}: thread delete failed ({err})")
                    continue

            if message is not None:
                try:
                    await message.delete()
                except discord.NotFound:
                    pass
                except (discord.Forbidden, discord.HTTPException) as err:
                    failures.append(f"{paper_id}: announcement delete failed ({err})")
                    continue

            MONGO[DBNAME][DISCUSSION_SUPPRESSION_COLLECTION].replace_one(
                {"_id": paper_id},
                {"_id": paper_id, "suppressed_at": now},
                upsert=True,
            )
            MONGO[DBNAME][DISCUSSION_COLLECTION].delete_one({"_id": paper_id})
            MONGO[DBNAME][PAPER_COLLECTION].delete_one({"_id": paper_id})
            purged += 1

        summary = [
            f"Purged {purged} discussion(s).",
            (
                "Purged papers are suppressed from auto-sync until "
                "`/create <eprint_id>` is used."
            ),
        ]
        if failures:
            summary.append(f"Failures: {len(failures)}")
            summary.extend(failures[:5])

        await interaction.followup.send("\n".join(summary), ephemeral=True)
