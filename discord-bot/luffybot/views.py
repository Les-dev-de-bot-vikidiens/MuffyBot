#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import contextlib
import datetime as dt
import json
import shutil
from pathlib import Path

import discord

from . import config
from .runtime import (
    apply_presence,
    backup_database,
    build_health_embed,
    dry_run_enabled,
    ensure_owner_interaction,
    export_runs_csv_file,
    is_public_channel_allowed,
    maybe_refresh_public_panel,
    process_queue,
    request_script_start,
    respond_ephemeral,
    restore_latest_backup,
    run_systemd_action,
    send_supervision,
    stop_script,
)
from .storage import (
    audit,
    filtered_runs,
    get_setting,
    get_setting_bool,
    get_setting_int,
    last_failed_run,
    last_runs,
    search_logs,
    set_setting,
    summarize_runs,
    init_db,
)
from .utils import fmt_duration, memory_used_percent, read_tail, utc_now


def build_public_panel_embed() -> discord.Embed:
    maintenance = get_setting_bool("maintenance_mode", False)
    public_enabled = get_setting_bool("public_start_enabled", True)
    dry_run = dry_run_enabled()
    max_parallel = get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)
    cooldown = get_setting_int("public_cooldown_seconds", 120, min_value=0, max_value=3600)
    used_pct = memory_used_percent()
    disk = shutil.disk_usage(str(config.PYWIKIBOT_DIR))

    lines_running: list[str] = []
    for key, running in sorted(config.RUNNING_SCRIPTS.items(), key=lambda item: item[1].run_id):
        elapsed = (utc_now() - running.started_at).total_seconds()
        lines_running.append(
            f"`{key}` pid={running.process.pid} {fmt_duration(elapsed)} retry={running.retry_index}"
        )
    running_text = "\n".join(lines_running[:10]) if lines_running else "Aucun script en cours."

    queue_lines = []
    ordered_queue = sorted(config.RUN_QUEUE, key=lambda i: (i.priority, i.enqueued_at, i.queue_id))
    for idx, item in enumerate(ordered_queue[:8], 1):
        queue_lines.append(f"{idx}. `{item.script_key}` prio={item.priority} retry={item.retry_index}")
    queue_text = "\n".join(queue_lines) if queue_lines else "Queue vide."

    rows = last_runs(None, limit=8)
    run_lines = [
        f"#{row['id']} {row['script_key']} {row['status']} rc={row['return_code']} dur={fmt_duration(row['duration_seconds'])}"
        for row in rows
    ]
    history_text = "\n".join(run_lines) if run_lines else "Aucun historique."

    embed = discord.Embed(title="Panneau Public Scripts", color=0x3498DB, timestamp=utc_now())
    embed.description = (
        f"Maintenance: `{'ON' if maintenance else 'OFF'}` | Start public: `{'ON' if public_enabled else 'OFF'}` | Dry-run: `{'ON' if dry_run else 'OFF'}`\n"
        f"Parallel: `{len(config.RUNNING_SCRIPTS)}/{max_parallel}` | Queue: `{len(config.RUN_QUEUE)}` | Cooldown: `{cooldown}s`"
    )
    embed.add_field(
        name="Ressources",
        value=(
            f"RAM utilisee `{used_pct:.1f}%` | "
            f"Disque libre `{disk.free // (1024**3)} GB`"
        ),
        inline=False,
    )
    embed.add_field(name="Scripts en cours", value=running_text[:1024], inline=False)
    embed.add_field(name="Queue", value=queue_text[:1024], inline=False)
    embed.add_field(name="Derniers runs", value=history_text[:1024], inline=False)
    return embed


def build_op_panel_embed() -> discord.Embed:
    maintenance = get_setting_bool("maintenance_mode", False)
    public_enabled = get_setting_bool("public_start_enabled", True)
    dry_run = dry_run_enabled()
    max_parallel = get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)
    cooldown = get_setting_int("public_cooldown_seconds", 120, min_value=0, max_value=3600)
    retries = get_setting_int("max_auto_retries", 1, min_value=0, max_value=5)
    backoff = get_setting_int("retry_backoff_seconds", 45, min_value=5, max_value=3600)

    state = get_setting("presence_state", "online")
    mode = get_setting("presence_mode", "watching")
    text = get_setting("presence_text", "Vikidia scripts | run:{running} queue:{queue}")

    embed = discord.Embed(title="Panneau OP (ephemere)", color=0xE67E22, timestamp=utc_now())
    embed.description = (
        f"Owner ID: `{config.OWNER_USER_ID}`\n"
        f"Maintenance: `{'ON' if maintenance else 'OFF'}` | Public start: `{'ON' if public_enabled else 'OFF'}` | Dry-run: `{'ON' if dry_run else 'OFF'}`\n"
        f"Parallel: `{len(config.RUNNING_SCRIPTS)}/{max_parallel}` | Queue: `{len(config.RUN_QUEUE)}` | Cooldown public: `{cooldown}s`\n"
        f"Retry: `{retries}` (backoff `{backoff}s`)\n"
        f"Presence: `{state}` `{mode}` text=`{text[:80]}`"
    )
    return embed


def build_runs_summary_embed(hours: int = 24) -> discord.Embed:
    now = utc_now()
    start = now - dt.timedelta(hours=max(1, min(hours, 24 * 30)))
    stats = summarize_runs(start.isoformat(), now.isoformat())

    by_status = ", ".join(f"{name}:{count}" for name, count in stats["by_status"][:8]) or "n/a"
    top_runs = ", ".join(f"{name}:{count}" for name, count in stats["by_script"][:8]) or "n/a"
    top_fails = ", ".join(f"{name}:{count}" for name, count in stats["by_script_failed"][:8]) or "n/a"

    embed = discord.Embed(
        title=f"Statistiques runs ({hours}h)",
        color=0x1ABC9C,
        timestamp=now,
    )
    embed.description = (
        f"Total: `{stats['total']}` | Success: `{stats['success_count']}` | "
        f"Echecs: `{stats['failure_count']}` | Success rate: `{stats['success_rate']:.1f}%`\n"
        f"Duree moyenne: `{fmt_duration(stats['avg_duration'])}`"
    )
    embed.add_field(name="Par statut", value=by_status[:1024], inline=False)
    embed.add_field(name="Top scripts (volume)", value=top_runs[:1024], inline=False)
    embed.add_field(name="Top scripts (echecs)", value=top_fails[:1024], inline=False)
    return embed


class PublicStartSelect(discord.ui.Select):
    def __init__(self) -> None:
        options = [
            discord.SelectOption(label=key, value=key, description=config.SCRIPT_DEFS[key].description[:90])
            for key in config.PUBLIC_SCRIPT_CHOICES
        ]
        super().__init__(
            placeholder="Lancer un script public (queue)",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=0,
            custom_id="public_panel_start_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        script = self.values[0]

        if get_setting_bool("maintenance_mode", False):
            await respond_ephemeral(interaction, "Mode maintenance actif: demarrage public desactive.")
            return
        if not get_setting_bool("public_start_enabled", True):
            await respond_ephemeral(interaction, "Le demarrage public est actuellement desactive.")
            return
        if not is_public_channel_allowed(interaction.channel_id):
            await respond_ephemeral(interaction, "Ce canal n'est pas autorise pour les starts publics.")
            return

        cooldown = get_setting_int("public_cooldown_seconds", 120, min_value=0, max_value=3600)
        now_mono = __import__("asyncio").get_running_loop().time()
        key = (int(interaction.user.id), script)
        previous = config.LAST_PUBLIC_START_MONO.get(key)
        if previous is not None and now_mono - previous < cooldown:
            remain = int(cooldown - (now_mono - previous))
            await respond_ephemeral(interaction, f"Cooldown actif pour `{script}`: reessaie dans {remain}s.")
            return

        try:
            result = await request_script_start(
                script_key=script,
                requester_id=interaction.user.id,
                requester_tag=str(interaction.user),
                channel_id=interaction.channel_id,
                public_request=True,
                bypass_limits=False,
                priority=5,
            )
        except Exception as exc:
            await respond_ephemeral(interaction, f"Impossible de lancer `{script}`: {exc}")
            return

        config.LAST_PUBLIC_START_MONO[key] = now_mono
        audit(
            interaction.user.id,
            "panel_public_start",
            script,
            json.dumps(result, ensure_ascii=False),
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )

        if result["state"] == "started":
            await respond_ephemeral(
                interaction,
                f"Script `{script}` lance immediatement. run_id=`{result['run_id']}` pid=`{result['pid']}`",
            )
        else:
            await respond_ephemeral(
                interaction,
                f"Script `{script}` ajoute en queue. queue_id=`{result['queue_id']}` position~`{result['position']}`",
            )

        await maybe_refresh_public_panel(force=True)
        await apply_presence()


class PublicStatusSelect(discord.ui.Select):
    def __init__(self) -> None:
        options = [discord.SelectOption(label=key, value=key) for key in config.PUBLIC_SCRIPT_CHOICES]
        super().__init__(
            placeholder="Voir le statut d'un script",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=1,
            custom_id="public_panel_status_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        script = self.values[0]
        running = config.RUNNING_SCRIPTS.get(script)
        last = last_runs(script, limit=1)

        queue_position: int | None = None
        ordered = sorted(config.RUN_QUEUE, key=lambda i: (i.priority, i.enqueued_at, i.queue_id))
        for idx, item in enumerate(ordered, 1):
            if item.script_key == script:
                queue_position = idx
                break

        embed = discord.Embed(title=f"Statut script: {script}", color=0x5865F2)
        if running:
            elapsed = (utc_now() - running.started_at).total_seconds()
            embed.add_field(name="En cours", value=f"Oui (pid {running.process.pid})", inline=True)
            embed.add_field(name="Depuis", value=fmt_duration(elapsed), inline=True)
            embed.add_field(name="run_id", value=str(running.run_id), inline=True)
        else:
            embed.add_field(name="En cours", value="Non", inline=True)

        embed.add_field(name="Queue", value=(str(queue_position) if queue_position else "Non"), inline=True)

        if last:
            row = last[0]
            embed.add_field(name="Dernier statut", value=row["status"], inline=True)
            embed.add_field(name="Dernier run_id", value=str(row["id"]), inline=True)
            embed.add_field(name="Duree", value=fmt_duration(row["duration_seconds"]), inline=True)
        else:
            embed.add_field(name="Historique", value="Aucun run enregistre", inline=False)

        audit(
            interaction.user.id,
            "panel_public_status",
            script,
            f"queue_pos={queue_position}",
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )
        await respond_ephemeral(interaction, embed=embed)


class PublicLogSelect(discord.ui.Select):
    def __init__(self) -> None:
        options = [discord.SelectOption(label=key, value=key) for key in config.PUBLIC_SCRIPT_CHOICES]
        super().__init__(
            placeholder="Voir le dernier log d'un script",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=2,
            custom_id="public_panel_log_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        script = self.values[0]
        rows = last_runs(script, limit=1)
        if not rows:
            await respond_ephemeral(interaction, "Aucun run trouve pour ce script.")
            return

        row = rows[0]
        path = Path(str(row["log_path"]))
        tail = read_tail(path, lines=100, max_chars=3500)
        header = f"run_id=#{row['id']} script={row['script_key']} status={row['status']}"
        payload = f"{header}\n{tail}".strip()

        audit(
            interaction.user.id,
            "panel_public_log",
            script,
            f"run_id={row['id']}",
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )

        if len(payload) <= 1800:
            await respond_ephemeral(interaction, f"```\n{payload}\n```")
            return

        tmp = config.RUN_LOG_DIR / f"tail_{row['id']}.txt"
        tmp.write_text(payload, encoding="utf-8")
        await respond_ephemeral(interaction, "Log en piece jointe:", file=discord.File(str(tmp), filename=tmp.name))


class PublicPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)
        self.add_item(PublicStartSelect())
        self.add_item(PublicStatusSelect())
        self.add_item(PublicLogSelect())

    @discord.ui.button(label="Rafraichir", style=discord.ButtonStyle.secondary, row=3, custom_id="public_panel_refresh")
    async def refresh_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        await maybe_refresh_public_panel(force=True)
        await respond_ephemeral(interaction, "Panneau public rafraichi.")

    @discord.ui.button(label="Health", style=discord.ButtonStyle.secondary, row=3, custom_id="public_panel_health")
    async def health_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        embed = build_health_embed("Sante publique")
        await respond_ephemeral(interaction, embed=embed)

    @discord.ui.button(label="Historique", style=discord.ButtonStyle.secondary, row=3, custom_id="public_panel_history")
    async def history_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        rows = last_runs(None, limit=12)
        if not rows:
            await respond_ephemeral(interaction, "Aucun historique.")
            return
        lines = [
            f"#{row['id']} {row['script_key']} {row['status']} rc={row['return_code']} dur={fmt_duration(row['duration_seconds'])}"
            for row in rows
        ]
        await respond_ephemeral(interaction, "\n".join(lines[:20]))

    @discord.ui.button(label="Queue", style=discord.ButtonStyle.secondary, row=3, custom_id="public_panel_queue")
    async def queue_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not config.RUN_QUEUE:
            await respond_ephemeral(interaction, "Queue vide.")
            return
        ordered = sorted(config.RUN_QUEUE, key=lambda i: (i.priority, i.enqueued_at, i.queue_id))
        lines = [f"{idx}. `{item.script_key}` prio={item.priority} retry={item.retry_index}" for idx, item in enumerate(ordered[:20], 1)]
        await respond_ephemeral(interaction, "\n".join(lines))


class OpStartSelect(discord.ui.Select):
    def __init__(self) -> None:
        options = [
            discord.SelectOption(label=key, value=key, description=config.SCRIPT_DEFS[key].description[:90])
            for key in config.ALL_SCRIPT_CHOICES
        ]
        super().__init__(
            placeholder="OP Start: choisir un script",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return

        script = self.values[0]
        try:
            result = await request_script_start(
                script_key=script,
                requester_id=interaction.user.id,
                requester_tag=str(interaction.user),
                channel_id=interaction.channel_id,
                public_request=False,
                bypass_limits=True,
                priority=1,
            )
        except Exception as exc:
            audit(
                interaction.user.id,
                "panel_op_start_failed",
                script,
                str(exc),
                guild_id=interaction.guild_id,
                channel_id=interaction.channel_id,
            )
            await respond_ephemeral(interaction, f"Echec lancement `{script}`: {exc}")
            return

        audit(
            interaction.user.id,
            "panel_op_start",
            script,
            json.dumps(result, ensure_ascii=False),
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )

        if result["state"] == "started":
            await respond_ephemeral(interaction, f"Lance: `{script}` run_id=`{result['run_id']}` pid=`{result['pid']}`")
        else:
            await respond_ephemeral(
                interaction,
                f"Ajoute en queue: `{script}` queue_id=`{result['queue_id']}` position~`{result['position']}`",
            )

        await maybe_refresh_public_panel(force=True)
        await apply_presence()


class OpStopSelect(discord.ui.Select):
    def __init__(self) -> None:
        running_keys = sorted(config.RUNNING_SCRIPTS.keys())
        if not running_keys:
            options = [discord.SelectOption(label="Aucun script actif", value="__none__")]
            disabled = True
        else:
            options = [discord.SelectOption(label=key, value=key) for key in running_keys[:25]]
            disabled = False
        super().__init__(
            placeholder="OP Stop: choisir un script actif",
            min_values=1,
            max_values=1,
            options=options,
            disabled=disabled,
            row=1,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return

        script = self.values[0]
        if script == "__none__":
            await respond_ephemeral(interaction, "Aucun script a arreter.")
            return

        ok = await stop_script(script, note="Arret demande depuis panneau OP")
        if ok:
            audit(interaction.user.id, "panel_op_stop", script, "requested", guild_id=interaction.guild_id, channel_id=interaction.channel_id)
            await respond_ephemeral(interaction, f"Stop demande pour `{script}`")
        else:
            await respond_ephemeral(interaction, f"Script `{script}` introuvable en cours.")

        await maybe_refresh_public_panel(force=True)


class OpServiceSelect(discord.ui.Select):
    def __init__(self) -> None:
        options: list[discord.SelectOption] = [
            discord.SelectOption(label="toggle maintenance", value="meta|toggle_maintenance"),
            discord.SelectOption(label="toggle public start", value="meta|toggle_public_start"),
            discord.SelectOption(label="toggle dry-run", value="meta|toggle_dry_run"),
            discord.SelectOption(label="clear queue", value="meta|clear_queue"),
            discord.SelectOption(label="digest now", value="meta|digest_now"),
        ]
        for service in config.ALLOWED_SYSTEMD_SERVICES:
            for action in ("status", "restart", "start", "stop"):
                options.append(discord.SelectOption(label=f"{action} {service}", value=f"svc|{action}|{service}"))

        super().__init__(
            placeholder="OP Actions systeme",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=2,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return

        parts = self.values[0].split("|", 2)
        if not parts:
            await respond_ephemeral(interaction, "Action invalide.")
            return

        if parts[0] == "meta":
            action = parts[1] if len(parts) > 1 else ""

            if action == "toggle_maintenance":
                enabled = not get_setting_bool("maintenance_mode", False)
                set_setting("maintenance_mode", "1" if enabled else "0")
                audit(interaction.user.id, "panel_op_set_maintenance", "maintenance_mode", str(int(enabled)), guild_id=interaction.guild_id, channel_id=interaction.channel_id)
                await respond_ephemeral(interaction, f"maintenance_mode={'ON' if enabled else 'OFF'}")
                await maybe_refresh_public_panel(force=True)
                return

            if action == "toggle_public_start":
                enabled = not get_setting_bool("public_start_enabled", True)
                set_setting("public_start_enabled", "1" if enabled else "0")
                audit(interaction.user.id, "panel_op_set_public_start", "public_start_enabled", str(int(enabled)), guild_id=interaction.guild_id, channel_id=interaction.channel_id)
                await respond_ephemeral(interaction, f"public_start_enabled={'ON' if enabled else 'OFF'}")
                await maybe_refresh_public_panel(force=True)
                return

            if action == "toggle_dry_run":
                enabled = not get_setting_bool("dry_run_mode", False)
                set_setting("dry_run_mode", "1" if enabled else "0")
                audit(interaction.user.id, "panel_op_set_dry_run", "dry_run_mode", str(int(enabled)), guild_id=interaction.guild_id, channel_id=interaction.channel_id)
                await respond_ephemeral(interaction, f"dry_run_mode={'ON' if enabled else 'OFF'}")
                await maybe_refresh_public_panel(force=True)
                return

            if action == "clear_queue":
                async with config.STATE_LOCK:
                    count = len(config.RUN_QUEUE)
                    config.RUN_QUEUE.clear()
                audit(interaction.user.id, "panel_op_clear_queue", "queue", f"cleared={count}", guild_id=interaction.guild_id, channel_id=interaction.channel_id)
                await respond_ephemeral(interaction, f"Queue videe ({count} element(s)).")
                await maybe_refresh_public_panel(force=True)
                await apply_presence()
                return

            if action == "refresh_presence":
                await apply_presence()
                audit(interaction.user.id, "panel_op_refresh_presence", "presence", "ok", guild_id=interaction.guild_id, channel_id=interaction.channel_id)
                await respond_ephemeral(interaction, "Presence rafraichie.")
                return

            if action == "digest_now":
                await send_supervision("Digest force demande via OP panel")
                audit(interaction.user.id, "panel_op_digest_now", "digest", "sent", guild_id=interaction.guild_id, channel_id=interaction.channel_id)
                await respond_ephemeral(interaction, "Digest force envoye (message test supervision).")
                return

            await respond_ephemeral(interaction, "Action meta inconnue.")
            return

        if len(parts) < 3:
            await respond_ephemeral(interaction, "Action systeme invalide.")
            return

        action = parts[1]
        service = parts[2]

        await interaction.response.defer(ephemeral=True)
        rc, output = await run_systemd_action(action, service)
        audit(interaction.user.id, "panel_op_service", service, f"action={action} rc={rc}", guild_id=interaction.guild_id, channel_id=interaction.channel_id)

        summary = f"systemctl {action} {service} -> rc={rc}"
        if len(output) > 1600:
            out_file = config.RUN_LOG_DIR / f"systemctl_{service}_{action}_{int(utc_now().timestamp())}.txt"
            out_file.write_text(output, encoding="utf-8")
            await interaction.followup.send(summary, ephemeral=True, file=discord.File(str(out_file), filename=out_file.name))
        else:
            await interaction.followup.send(f"{summary}\n```\n{output[-1500:]}\n```", ephemeral=True)


class OpsConfigModal(discord.ui.Modal):
    def __init__(self) -> None:
        super().__init__(title="Config OP")

        self.max_parallel = discord.ui.InputText(
            label="max_parallel_runs (1-20)",
            required=True,
            max_length=2,
            value=str(get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)),
        )
        self.cooldown = discord.ui.InputText(
            label="public_cooldown_seconds (0-3600)",
            required=True,
            max_length=4,
            value=str(get_setting_int("public_cooldown_seconds", 120, min_value=0, max_value=3600)),
        )
        self.retry = discord.ui.InputText(
            label="retry,max_backoff Ex: 1,45",
            required=True,
            max_length=16,
            value=f"{get_setting_int('max_auto_retries', 1, 0, 5)},{get_setting_int('retry_backoff_seconds', 45, 5, 3600)}",
        )
        self.limits = discord.ui.InputText(
            label="procMB,ram%,loadx10,freeGB",
            required=True,
            max_length=40,
            value=(
                f"{get_setting_int('max_process_ram_mb', 1400, 64, 32768)},"
                f"{get_setting_int('max_system_ram_percent', 92, 50, 99)},"
                f"{get_setting_int('max_load_per_cpu_x10', 30, 5, 120)},"
                f"{get_setting_int('min_free_disk_gb', 2, 0, 500)}"
            ),
        )
        self.whitelist = discord.ui.InputText(
            label="Whitelist canaux (ids csv, vide=tous)",
            required=False,
            max_length=200,
            value=get_setting("public_channel_whitelist", ""),
        )

        self.add_item(self.max_parallel)
        self.add_item(self.cooldown)
        self.add_item(self.retry)
        self.add_item(self.limits)
        self.add_item(self.whitelist)

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return

        try:
            max_parallel = max(min(int(str(self.max_parallel.value).strip()), 20), 1)
            cooldown = max(min(int(str(self.cooldown.value).strip()), 3600), 0)

            retry_raw = [x.strip() for x in str(self.retry.value).split(",")]
            retries = max(min(int(retry_raw[0]), 5), 0)
            backoff = max(min(int(retry_raw[1]), 3600), 5)

            limits_raw = [x.strip() for x in str(self.limits.value).split(",")]
            proc_mb = max(min(int(limits_raw[0]), 32768), 64)
            ram_pct = max(min(int(limits_raw[1]), 99), 50)
            load_x10 = max(min(int(limits_raw[2]), 120), 5)
            free_gb = max(min(int(limits_raw[3]), 500), 0)

            whitelist = str(self.whitelist.value or "").strip().replace(";", ",")
        except Exception as exc:
            await respond_ephemeral(interaction, f"Valeurs invalides: {exc}")
            return

        set_setting("max_parallel_runs", str(max_parallel))
        set_setting("public_cooldown_seconds", str(cooldown))
        set_setting("max_auto_retries", str(retries))
        set_setting("retry_backoff_seconds", str(backoff))
        set_setting("max_process_ram_mb", str(proc_mb))
        set_setting("max_system_ram_percent", str(ram_pct))
        set_setting("max_load_per_cpu_x10", str(load_x10))
        set_setting("min_free_disk_gb", str(free_gb))
        set_setting("public_channel_whitelist", whitelist)

        audit(
            interaction.user.id,
            "panel_op_set_config",
            "settings",
            (
                f"max_parallel={max_parallel} cooldown={cooldown} retries={retries} backoff={backoff} "
                f"proc_mb={proc_mb} ram_pct={ram_pct} load_x10={load_x10} free_gb={free_gb} whitelist={whitelist}"
            ),
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )

        await respond_ephemeral(
            interaction,
            "Configuration mise a jour:\n"
            f"max_parallel={max_parallel}\n"
            f"cooldown={cooldown}\n"
            f"retries={retries} backoff={backoff}\n"
            f"proc_mb={proc_mb} ram_pct={ram_pct} load_x10={load_x10} free_gb={free_gb}\n"
            f"whitelist={whitelist or '(vide)'}",
        )
        await maybe_refresh_public_panel(force=True)


class BotStatusModal(discord.ui.Modal):
    def __init__(self) -> None:
        super().__init__(title="Statut du bot")
        self.state = discord.ui.InputText(
            label="Etat: online|idle|dnd|invisible",
            required=True,
            max_length=16,
            value=get_setting("presence_state", "online"),
        )
        self.mode = discord.ui.InputText(
            label="Mode: watching|playing|listening|competing",
            required=True,
            max_length=16,
            value=get_setting("presence_mode", "watching"),
        )
        self.text = discord.ui.InputText(
            label="Texte (vars: {running} {queue})",
            required=True,
            max_length=128,
            value=get_setting("presence_text", "Vikidia scripts | run:{running} queue:{queue}"),
        )

        self.add_item(self.state)
        self.add_item(self.mode)
        self.add_item(self.text)

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return

        state = str(self.state.value).strip().lower()
        mode = str(self.mode.value).strip().lower()
        text = str(self.text.value).strip()

        if state not in {"online", "idle", "dnd", "invisible", "offline"}:
            await respond_ephemeral(interaction, "Etat invalide.")
            return
        if mode not in {"watching", "playing", "listening", "competing"}:
            await respond_ephemeral(interaction, "Mode invalide.")
            return
        if not text:
            await respond_ephemeral(interaction, "Texte invalide.")
            return

        set_setting("presence_state", "invisible" if state == "offline" else state)
        set_setting("presence_mode", mode)
        set_setting("presence_text", text)

        await apply_presence()
        audit(
            interaction.user.id,
            "panel_op_set_presence",
            "presence",
            f"state={state} mode={mode} text={text}",
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )
        await respond_ephemeral(interaction, f"Presence mise a jour: {state} {mode} '{text}'")


class RunFilterModal(discord.ui.Modal):
    def __init__(self) -> None:
        super().__init__(title="Filtrer les runs")
        self.script = discord.ui.InputText(label="script (vide = tous)", required=False, max_length=64, value="")
        self.status = discord.ui.InputText(label="status (vide = tous)", required=False, max_length=32, value="")
        self.page_size = discord.ui.InputText(label="page_size (1-25)", required=True, max_length=2, value="10")

        self.add_item(self.script)
        self.add_item(self.status)
        self.add_item(self.page_size)

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return

        script = str(self.script.value or "").strip() or None
        status = str(self.status.value or "").strip() or None
        try:
            page_size = max(min(int(str(self.page_size.value).strip()), 25), 1)
        except ValueError:
            await respond_ephemeral(interaction, "page_size invalide")
            return

        view = RunsPagerView(owner_id=interaction.user.id, script_filter=script, status_filter=status, page_size=page_size)
        embed = view.make_embed()
        audit(
            interaction.user.id,
            "panel_op_runs_filter",
            "runs",
            f"script={script} status={status} page_size={page_size}",
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )
        await respond_ephemeral(interaction, embed=embed, view=view)


class LogSearchModal(discord.ui.Modal):
    def __init__(self) -> None:
        super().__init__(title="Recherche logs")
        self.script = discord.ui.InputText(label="script (vide = tous)", required=False, max_length=64, value="")
        self.pattern = discord.ui.InputText(label="pattern a chercher", required=True, max_length=120, value="")
        self.max_lines = discord.ui.InputText(label="max lignes (1-200)", required=True, max_length=3, value="40")

        self.add_item(self.script)
        self.add_item(self.pattern)
        self.add_item(self.max_lines)

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return

        script = str(self.script.value or "").strip() or None
        pattern = str(self.pattern.value or "").strip()
        try:
            max_lines = max(min(int(str(self.max_lines.value).strip()), 200), 1)
        except ValueError:
            await respond_ephemeral(interaction, "max_lines invalide")
            return

        rows = last_runs(script, limit=80) if script else last_runs(None, limit=120)
        paths = [(int(row["id"]), str(row["script_key"]), str(row["log_path"])) for row in rows]
        result = search_logs(paths, pattern, max_lines)

        audit(
            interaction.user.id,
            "panel_op_log_search",
            script or "*",
            f"pattern={pattern} max_lines={max_lines}",
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )

        if len(result) <= 1800:
            await respond_ephemeral(interaction, f"```\n{result}\n```")
            return

        tmp = config.RUN_LOG_DIR / f"log_search_{int(utc_now().timestamp())}.txt"
        tmp.write_text(result, encoding="utf-8")
        await respond_ephemeral(interaction, "Resultat en piece jointe", file=discord.File(str(tmp), filename=tmp.name))


class RunsPagerView(discord.ui.View):
    def __init__(self, owner_id: int, script_filter: str | None, status_filter: str | None, page_size: int) -> None:
        super().__init__(timeout=900)
        self.owner_id = owner_id
        self.script_filter = script_filter
        self.status_filter = status_filter
        self.page_size = page_size
        self.offset = 0
        self.total = 0

    def _page_rows(self):
        rows, total = filtered_runs(
            script_key=self.script_filter,
            status=self.status_filter,
            limit=self.page_size,
            offset=self.offset,
        )
        self.total = total
        return rows

    def make_embed(self) -> discord.Embed:
        rows = self._page_rows()
        title = "Runs"
        if self.script_filter or self.status_filter:
            title = f"Runs (script={self.script_filter or '*'} status={self.status_filter or '*'})"

        embed = discord.Embed(title=title, color=0x95A5A6, timestamp=utc_now())
        if not rows:
            embed.description = "Aucun resultat."
        else:
            lines = [
                f"#{row['id']} {row['script_key']} {row['status']} rc={row['return_code']} dur={fmt_duration(row['duration_seconds'])}"
                for row in rows
            ]
            embed.description = "\n".join(lines)

        page_no = (self.offset // self.page_size) + 1
        page_total = max(1, (self.total + self.page_size - 1) // self.page_size)
        embed.set_footer(text=f"page {page_no}/{page_total} | total {self.total}")

        self.prev_btn.disabled = self.offset <= 0
        self.next_btn.disabled = self.offset + self.page_size >= self.total
        return embed

    async def _check_owner(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) == int(self.owner_id):
            return True
        await respond_ephemeral(interaction, "View reservee a son auteur.")
        return False

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await self._check_owner(interaction):
            return
        self.offset = max(0, self.offset - self.page_size)
        await interaction.response.edit_message(embed=self.make_embed(), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await self._check_owner(interaction):
            return
        self.offset = min(self.offset + self.page_size, max(self.total - 1, 0))
        await interaction.response.edit_message(embed=self.make_embed(), view=self)


class BackupManageView(discord.ui.View):
    def __init__(self, owner_id: int) -> None:
        super().__init__(timeout=300)
        self.owner_id = owner_id

    async def _check_owner(self, interaction: discord.Interaction) -> bool:
        if int(interaction.user.id) == int(self.owner_id):
            return True
        await respond_ephemeral(interaction, "View reservee a son auteur.")
        return False

    @discord.ui.button(label="Backup now", style=discord.ButtonStyle.primary)
    async def backup_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await self._check_owner(interaction):
            return
        try:
            path = backup_database()
        except Exception as exc:
            await respond_ephemeral(interaction, f"Backup echec: {exc}")
            return
        audit(interaction.user.id, "panel_op_db_backup", "db", str(path), guild_id=interaction.guild_id, channel_id=interaction.channel_id)
        await respond_ephemeral(interaction, f"Backup cree: `{path.name}`")

    @discord.ui.button(label="Restore latest", style=discord.ButtonStyle.danger)
    async def restore_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await self._check_owner(interaction):
            return

        async with config.STATE_LOCK:
            running = len(config.RUNNING_SCRIPTS)
        if running:
            await respond_ephemeral(interaction, "Stoppe les scripts actifs avant restore.")
            return

        try:
            backup = restore_latest_backup()
            init_db()
        except Exception as exc:
            await respond_ephemeral(interaction, f"Restore echec: {exc}")
            return

        audit(interaction.user.id, "panel_op_db_restore", "db", str(backup), guild_id=interaction.guild_id, channel_id=interaction.channel_id)
        await respond_ephemeral(interaction, f"Restore applique depuis `{backup.name}`")
        await maybe_refresh_public_panel(force=True)


class OpPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=900)
        self.add_item(OpStartSelect())
        self.add_item(OpStopSelect())
        self.add_item(OpServiceSelect())

    @discord.ui.button(label="Config", style=discord.ButtonStyle.secondary, row=3)
    async def config_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        await interaction.response.send_modal(OpsConfigModal())

    @discord.ui.button(label="Runs", style=discord.ButtonStyle.secondary, row=3)
    async def runs_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        view = RunsPagerView(owner_id=interaction.user.id, script_filter=None, status_filter=None, page_size=10)
        await respond_ephemeral(interaction, embed=view.make_embed(), view=view)

    @discord.ui.button(label="Filter Runs", style=discord.ButtonStyle.secondary, row=3)
    async def filter_runs_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        await interaction.response.send_modal(RunFilterModal())

    @discord.ui.button(label="Search Logs", style=discord.ButtonStyle.secondary, row=3)
    async def search_logs_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        await interaction.response.send_modal(LogSearchModal())

    @discord.ui.button(label="Export CSV", style=discord.ButtonStyle.secondary, row=3)
    async def export_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        try:
            path = export_runs_csv_file(days=30)
        except Exception as exc:
            await respond_ephemeral(interaction, f"Export echec: {exc}")
            return

        audit(interaction.user.id, "panel_op_export_csv", "runs", str(path), guild_id=interaction.guild_id, channel_id=interaction.channel_id)
        await respond_ephemeral(interaction, "Export CSV:", file=discord.File(str(path), filename=path.name))

    @discord.ui.button(label="Stop All", style=discord.ButtonStyle.danger, row=4)
    async def stop_all_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        async with config.STATE_LOCK:
            keys = list(config.RUNNING_SCRIPTS.keys())
        stopped = 0
        for key in keys:
            if await stop_script(key, note="Stop all demande depuis panneau OP"):
                stopped += 1

        audit(interaction.user.id, "panel_op_stop_all", "*", f"stopped={stopped}", guild_id=interaction.guild_id, channel_id=interaction.channel_id)
        await interaction.followup.send(f"Stop demande pour {stopped} script(s).", ephemeral=True)
        await maybe_refresh_public_panel(force=True)

    @discord.ui.button(label="Panic Stop", style=discord.ButtonStyle.danger, row=4)
    async def panic_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        async with config.STATE_LOCK:
            targets = [key for key in config.RUNNING_SCRIPTS.keys() if not config.SCRIPT_DEFS[key].critical]
        stopped = 0
        for key in targets:
            if await stop_script(key, note="Panic stop OP (non-critical)"):
                stopped += 1

        audit(interaction.user.id, "panel_op_panic_stop", "non-critical", f"stopped={stopped}", guild_id=interaction.guild_id, channel_id=interaction.channel_id)
        await interaction.followup.send(f"Panic stop effectue: {stopped} script(s) non-critiques arretes.", ephemeral=True)
        await maybe_refresh_public_panel(force=True)

    @discord.ui.button(label="Restart Failed", style=discord.ButtonStyle.primary, row=4)
    async def restart_failed_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return

        row = last_failed_run()
        if not row:
            await respond_ephemeral(interaction, "Aucun run en echec a relancer.")
            return

        script = str(row["script_key"])
        try:
            result = await request_script_start(
                script_key=script,
                requester_id=interaction.user.id,
                requester_tag=str(interaction.user),
                channel_id=interaction.channel_id,
                public_request=False,
                bypass_limits=True,
                priority=1,
            )
        except Exception as exc:
            await respond_ephemeral(interaction, f"Relance impossible: {exc}")
            return

        audit(
            interaction.user.id,
            "panel_op_restart_last_failed",
            script,
            json.dumps({"from_run": row["id"], **result}, ensure_ascii=False),
            guild_id=interaction.guild_id,
            channel_id=interaction.channel_id,
        )

        if result["state"] == "started":
            await respond_ephemeral(interaction, f"Relance immediate de `{script}`. run_id={result['run_id']} pid={result['pid']}")
        else:
            await respond_ephemeral(interaction, f"Relance queuee de `{script}`. queue_id={result['queue_id']} pos~{result['position']}")

        await maybe_refresh_public_panel(force=True)

    @discord.ui.button(label="Backup/Restore", style=discord.ButtonStyle.secondary, row=4)
    async def backup_manage_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        view = BackupManageView(owner_id=interaction.user.id)
        await respond_ephemeral(interaction, "Gestion DB:", view=view)

    @discord.ui.button(label="Bot Status", style=discord.ButtonStyle.primary, row=4)
    async def bot_status_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        await interaction.response.send_modal(BotStatusModal())
