#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import contextlib
import traceback

import discord

from . import config
from .runtime import (
    apply_presence,
    ensure_background_tasks_started,
    is_owner,
    is_public_channel_allowed,
    register_panel_refresh_callback,
    send_supervision,
)
from .storage import (
    audit,
    clear_public_panel_location,
    get_public_panel_location,
    init_db,
    set_public_panel_location,
)
from .views import OpPanelView, PublicPanelView, build_op_panel_embed, build_public_panel_embed


async def ensure_owner_ctx(ctx: discord.ApplicationContext) -> bool:
    if is_owner(ctx.author.id):
        return True
    await ctx.respond("Commande reservee au proprietaire.", ephemeral=True)
    return False


async def refresh_saved_public_panel() -> bool:
    channel_id, message_id = get_public_panel_location()
    if not channel_id or not message_id:
        return False

    try:
        channel = config.bot.get_channel(channel_id)
        if channel is None:
            channel = await config.bot.fetch_channel(channel_id)
        message = await channel.fetch_message(message_id)
        await message.edit(embed=build_public_panel_embed(), view=PublicPanelView())
        return True
    except Exception:
        clear_public_panel_location()
        return False


async def upsert_public_panel(channel: discord.abc.Messageable) -> tuple[discord.Message, bool]:
    channel_id, message_id = get_public_panel_location()
    if channel_id and message_id and getattr(channel, "id", None) == channel_id:
        try:
            message = await channel.fetch_message(message_id)
            await message.edit(embed=build_public_panel_embed(), view=PublicPanelView())
            return message, False
        except Exception:
            clear_public_panel_location()

    message = await channel.send(embed=build_public_panel_embed(), view=PublicPanelView())
    set_public_panel_location(message.channel.id, message.id)
    return message, True


register_panel_refresh_callback(refresh_saved_public_panel)


@config.bot.event
async def on_ready() -> None:
    init_db()
    ensure_background_tasks_started()

    config.LOGGER.info("Connecte: %s (%s)", config.bot.user, config.bot.user.id if config.bot.user else "?")
    await apply_presence()

    restored = await refresh_saved_public_panel()
    if restored:
        config.LOGGER.info("Panneau public restaure.")

    with contextlib.suppress(Exception):
        await send_supervision(
            f"Bot pret\nuser={config.bot.user}\nrunning={len(config.RUNNING_SCRIPTS)} queue={len(config.RUN_QUEUE)}"
        )


@config.bot.event
async def on_application_command_error(interaction: discord.Interaction, error: Exception) -> None:
    config.LOGGER.exception("Erreur commande: %s", error)
    message = f"Erreur: {error.__class__.__name__}: {error}"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except Exception:
        pass

    details = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    with contextlib.suppress(Exception):
        await send_supervision(
            f"Commande en erreur\n"
            f"user={interaction.user} ({interaction.user.id if interaction.user else '?'})\n"
            f"command={interaction.command}\n"
            f"error={details}"
        )


@config.bot.slash_command(name="panel", description="Panneau public pour gerer les scripts")
async def panel(ctx: discord.ApplicationContext) -> None:
    if not is_public_channel_allowed(ctx.channel_id) and not is_owner(ctx.author.id):
        await ctx.respond("Ce canal n'est pas autorise pour le panneau public.", ephemeral=True)
        return

    message, created = await upsert_public_panel(ctx.channel)
    audit(
        ctx.author.id,
        "panel_upsert",
        str(message.id),
        f"created={int(created)} channel={ctx.channel_id}",
        guild_id=ctx.guild_id,
        channel_id=ctx.channel_id,
    )
    await ctx.respond(
        f"Panneau public {'cree' if created else 'mis a jour'}: {message.jump_url}",
        ephemeral=True,
    )


@config.bot.slash_command(name="op_panel", description="Panneau OP (owner) en ephemere")
async def op_panel(ctx: discord.ApplicationContext) -> None:
    if not await ensure_owner_ctx(ctx):
        return

    audit(
        ctx.author.id,
        "panel_op_open",
        "op_panel",
        "opened",
        guild_id=ctx.guild_id,
        channel_id=ctx.channel_id,
    )
    await ctx.respond(embed=build_op_panel_embed(), view=OpPanelView(), ephemeral=True)


def register_commands() -> None:
    return
