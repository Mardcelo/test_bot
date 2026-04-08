import asyncio
import logging

import aiohttp
import discord
from discord import app_commands

from config import DBNAME, DISCUSSION_SUPPRESSION_COLLECTION, MONGO
from lib.discord_util import add_discussion_member
from lib.eprint.scraper import fetch_paper_by_id, normalize_eprint_id
from lib.eprint.sync import (
    ensure_discussion_for_paper,
    resolve_thread,
    upsert_paper_record,
)
from lib.util import get_paper_info
from msg_components.buttons.discussion import LeaveDiscussionButton

_log = logging.getLogger("discord.eruditus.cmds.create")


class Create(app_commands.Command):
    def __init__(self) -> None:
        super().__init__(
            name="create",
            description="Create a discussion for any ePrint paper by ID or URL.",
            callback=self.cmd_callback,  # type: ignore
        )

    @app_commands.checks.bot_has_permissions(manage_channels=True)
    async def cmd_callback(
        self, interaction: discord.Interaction, eprint_id: str
    ) -> None:
        """Create or refresh a discussion for a specific ePrint paper."""
        normalized_id = normalize_eprint_id(eprint_id)
        if normalized_id is None:
            await interaction.response.send_message(
                "Use an ePrint paper ID like `2024/139` or a matching ePrint URL.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        existing_paper = get_paper_info(_id=normalized_id)
        try:
            fetched_paper = await fetch_paper_by_id(normalized_id)
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            _log.warning("Failed to fetch ePrint paper %s: %s", normalized_id, error)
            fetched_paper = None

        if fetched_paper is None and existing_paper is None:
            await interaction.followup.send(
                f"Could not find ePrint paper `{normalized_id}`.", ephemeral=True
            )
            return

        MONGO[DBNAME][DISCUSSION_SUPPRESSION_COLLECTION].delete_one(
            {"_id": normalized_id}
        )
        paper_doc, _ = upsert_paper_record(fetched_paper or existing_paper)
        discussion, created = await ensure_discussion_for_paper(
            interaction.client, interaction.guild, paper_doc
        )
        if discussion is None:
            await interaction.followup.send(
                "This paper is withdrawn, so no new discussion was opened.",
                ephemeral=True,
            )
            return

        thread = await resolve_thread(
            interaction.client, interaction.guild, discussion["thread_id"]
        )
        if thread is None:
            await interaction.followup.send(
                "The paper was saved, but the discussion thread could not be created.",
                ephemeral=True,
            )
            return

        joined = interaction.user.id in discussion["member_ids"]
        if not joined:
            await add_discussion_member(thread, discussion, interaction.user)
            await thread.send(
                (
                    f"{interaction.user.mention} requested this paper and joined "
                    "the discussion."
                )
            )

        verb = "Created" if created else "Updated"
        join_suffix = (
            " You were added to the discussion."
            if not joined
            else " You were already in the discussion."
        )
        await interaction.followup.send(
            (
                f"{verb} discussion for `{paper_doc['_id']}`.\n"
                f"Thread: {thread.mention}.{join_suffix}"
            ),
            view=LeaveDiscussionButton(paper_id=paper_doc["_id"]),
            ephemeral=True,
        )
