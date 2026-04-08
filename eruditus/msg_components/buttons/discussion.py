import discord

from config import DBNAME, DISCUSSION_COLLECTION, MONGO
from lib.discord_util import add_discussion_member, remove_discussion_member
from lib.util import get_discussion_info, get_paper_info


async def _resolve_thread(
    interaction: discord.Interaction, thread_id: int | None
) -> discord.Thread | None:
    if not thread_id:
        return None

    thread = interaction.guild.get_thread(thread_id)
    if isinstance(thread, discord.Thread):
        return thread

    cached = interaction.client.get_channel(thread_id)
    if isinstance(cached, discord.Thread):
        return cached

    try:
        fetched = await interaction.client.fetch_channel(thread_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return None

    return fetched if isinstance(fetched, discord.Thread) else None


class _DiscussionButton(discord.ui.Button):
    def __init__(self, paper_id: str, disabled: bool = False) -> None:
        super().__init__(
            style=discord.ButtonStyle.green,
            custom_id=f"discussion::{paper_id}",
            disabled=disabled,
            label="Discussion closed." if disabled else "Join Discussion",
        )
        self.paper_id = paper_id

    async def callback(self, interaction: discord.Interaction) -> None:
        discussion = get_discussion_info(_id=self.paper_id)
        paper = get_paper_info(_id=self.paper_id)
        if discussion is None or paper is None:
            await interaction.response.send_message(
                "No discussion exists for this paper yet.", ephemeral=True
            )
            return

        if paper["withdrawn"] or discussion["status"] != "active":
            await interaction.response.send_message(
                "This discussion is closed.", ephemeral=True
            )
            return

        if interaction.user.id in discussion["member_ids"]:
            await interaction.response.send_message(
                "You already joined this discussion.", ephemeral=True
            )
            return

        thread = await _resolve_thread(interaction, discussion["thread_id"])
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
            f"Added you to the discussion for `{paper['title']}`.",
            view=LeaveDiscussionButton(paper_id=self.paper_id),
            ephemeral=True,
        )
        await thread.send(f"{interaction.user.mention} joined the discussion.")


class DiscussionButton(discord.ui.View):
    def __init__(self, paper_id: str, disabled: bool = False) -> None:
        super().__init__(timeout=None)
        self.add_item(_DiscussionButton(paper_id=paper_id, disabled=disabled))


class _LeaveDiscussionButton(discord.ui.Button):
    def __init__(self, paper_id: str) -> None:
        super().__init__(
            style=discord.ButtonStyle.red,
            label="Leave Discussion",
        )
        self.paper_id = paper_id

    async def callback(self, interaction: discord.Interaction) -> None:
        discussion = get_discussion_info(_id=self.paper_id)
        paper = get_paper_info(_id=self.paper_id)
        if discussion is None or paper is None:
            await interaction.response.edit_message(
                content="No discussion exists for this paper.", view=None
            )
            return

        if interaction.user.id not in discussion["member_ids"]:
            await interaction.response.edit_message(
                content="You are not in this discussion.", view=None
            )
            return

        thread = await _resolve_thread(interaction, discussion["thread_id"])
        if thread is None:
            discussion["member_ids"].remove(interaction.user.id)
            MONGO[DBNAME][DISCUSSION_COLLECTION].update_one(
                {"_id": discussion["_id"]},
                {"$set": {"member_ids": discussion["member_ids"]}},
            )
            await interaction.response.edit_message(
                content=(
                    "Removed you from the discussion state, "
                    "but the thread is missing."
                ),
                view=None,
            )
            return

        await remove_discussion_member(thread, discussion, interaction.user)
        await thread.send(f"{interaction.user.mention} left the discussion.")
        await interaction.response.edit_message(
            content=f"Removed you from the discussion for `{paper['title']}`.",
            view=None,
        )


class LeaveDiscussionButton(discord.ui.View):
    def __init__(self, paper_id: str) -> None:
        super().__init__(timeout=None)
        self.add_item(_LeaveDiscussionButton(paper_id=paper_id))
