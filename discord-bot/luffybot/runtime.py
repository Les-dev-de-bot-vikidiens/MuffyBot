#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import contextlib
import csv
import datetime as dt
import json
import os
import shutil
import subprocess
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import discord

from . import config
from .storage import (
    audit,
    db_connect,
    finalize_run,
    get_setting,
    get_setting_bool,
    get_setting_int,
    insert_run,
    last_runs,
    server_log,
    set_setting,
    summarize_runs,
)
from .utils import (
    fmt_duration,
    load_per_cpu,
    memory_stats_mb,
    memory_used_percent,
    parse_int_csv,
    process_rss_mb,
    redact_sensitive,
    utc_now,
    utc_now_iso,
)

PANEL_REFRESH_CALLBACK: Callable[[], Awaitable[bool]] | None = None


def register_panel_refresh_callback(callback: Callable[[], Awaitable[bool]]) -> None:
    global PANEL_REFRESH_CALLBACK
    PANEL_REFRESH_CALLBACK = callback


async def maybe_refresh_public_panel(force: bool = False) -> bool:
    if PANEL_REFRESH_CALLBACK is None:
        return False

    loop = asyncio.get_running_loop()
    now_mono = loop.time()
    if not force and now_mono - config.LAST_PANEL_REFRESH_MONO < 1.2:
        config.PANEL_DIRTY = True
        return False

    if not force and not config.PANEL_DIRTY and now_mono - config.LAST_PANEL_REFRESH_MONO < 5.0:
        return False

    config.PANEL_DIRTY = False
    config.LAST_PANEL_REFRESH_MONO = now_mono
    try:
        return await PANEL_REFRESH_CALLBACK()
    except Exception as exc:
        config.LOGGER.exception("Panel refresh callback failed: %s", exc)
        return False


def mark_panel_dirty() -> None:
    config.PANEL_DIRTY = True


def dry_run_enabled() -> bool:
    raw = os.getenv("MUFFYBOT_DRY_RUN", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    return get_setting_bool("dry_run_mode", False)


def kill_switch_enabled() -> bool:
    return get_setting_bool("kill_switch_mode", False) or config.KILL_SWITCH_FILE.exists()


def maintenance_mode_enabled() -> bool:
    return get_setting_bool("maintenance_mode", False) or config.MAINTENANCE_FILE.exists()


def _write_control_file(path: Path, reason: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"reason={reason[:240]}\nts={utc_now_iso()}\n", encoding="utf-8")


def sync_control_files() -> None:
    if kill_switch_enabled():
        _write_control_file(config.KILL_SWITCH_FILE, "enabled_from_luffybot")
    else:
        config.KILL_SWITCH_FILE.unlink(missing_ok=True)

    if maintenance_mode_enabled():
        _write_control_file(config.MAINTENANCE_FILE, "enabled_from_luffybot")
    else:
        config.MAINTENANCE_FILE.unlink(missing_ok=True)


def load_token() -> str:
    token = (os.getenv("DISCORD_TOKEN") or "").strip()
    if token:
        return token
    if config.TOKEN_FILE.exists():
        return config.TOKEN_FILE.read_text(encoding="utf-8").strip()
    raise RuntimeError("DISCORD_TOKEN manquant (env ou token.txt)")


def is_owner(user_id: int) -> bool:
    return int(user_id) == config.OWNER_USER_ID


def is_public_channel_allowed(channel_id: int | None) -> bool:
    if channel_id is None:
        return False
    whitelist = parse_int_csv(get_setting("public_channel_whitelist", ""))
    if not whitelist:
        return True
    return int(channel_id) in whitelist


async def supervision_channel() -> discord.abc.Messageable | None:
    channel_id = get_setting_int("digest_channel_id", config.SUPERVISION_CHANNEL_ID, min_value=1, max_value=10**20)
    channel = config.bot.get_channel(channel_id)
    if channel:
        return channel
    try:
        return await config.bot.fetch_channel(channel_id)
    except Exception:
        return None


async def send_supervision(message: str) -> None:
    channel = await supervision_channel()
    if not channel:
        config.LOGGER.warning("Supervision channel unavailable")
        return

    safe = redact_sensitive(message)
    if len(safe) <= 1900:
        await channel.send(f"```\n{safe}\n```")
        return

    path = config.RUN_LOG_DIR / f"supervision_{int(utc_now().timestamp())}.txt"
    path.write_text(safe, encoding="utf-8")
    await channel.send("Supervision log:", file=discord.File(str(path), filename=path.name))


async def critical_alert(message: str) -> None:
    user_id = get_setting_int("critical_mention_user_id", config.OWNER_USER_ID, min_value=1, max_value=10**20)
    await send_supervision(f"<@{user_id}> CRITICAL\n{message}")


async def respond_ephemeral(
    interaction: discord.Interaction,
    content: str | None = None,
    *,
    file: discord.File | None = None,
    embed: discord.Embed | None = None,
    view: discord.ui.View | None = None,
) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(content=content, ephemeral=True, file=file, embed=embed, view=view)
    else:
        await interaction.response.send_message(content=content, ephemeral=True, file=file, embed=embed, view=view)


async def ensure_owner_interaction(interaction: discord.Interaction) -> bool:
    if is_owner(interaction.user.id):
        return True
    await respond_ephemeral(interaction, "Action reservee au proprietaire.")
    return False


def presence_status_from_string(raw: str) -> discord.Status:
    key = (raw or "online").strip().lower()
    mapping = {
        "online": discord.Status.online,
        "idle": discord.Status.idle,
        "dnd": discord.Status.dnd,
        "invisible": discord.Status.invisible,
        "offline": discord.Status.invisible,
    }
    return mapping.get(key, discord.Status.online)


def presence_activity_from_settings() -> discord.Activity:
    mode = get_setting("presence_mode", "watching").strip().lower()
    template = get_setting("presence_text", "Vikidia scripts | run:{running} queue:{queue}")
    text = template.format(running=len(config.RUNNING_SCRIPTS), queue=len(config.RUN_QUEUE))[:128]

    mode_map = {
        "playing": discord.ActivityType.playing,
        "watching": discord.ActivityType.watching,
        "listening": discord.ActivityType.listening,
        "competing": discord.ActivityType.competing,
    }
    activity_type = mode_map.get(mode, discord.ActivityType.watching)
    return discord.Activity(type=activity_type, name=text)


async def apply_presence() -> None:
    if not config.bot.user:
        return
    status = presence_status_from_string(get_setting("presence_state", "online"))
    activity = presence_activity_from_settings()
    with contextlib.suppress(Exception):
        await config.bot.change_presence(status=status, activity=activity)


def queued_script_keys() -> set[str]:
    return {item.script_key for item in config.RUN_QUEUE}


def queue_lines(limit: int = 10) -> list[str]:
    ordered = sorted(config.RUN_QUEUE, key=lambda i: (i.priority, i.enqueued_at, i.queue_id))
    lines: list[str] = []
    now = utc_now()
    for idx, item in enumerate(ordered[:limit], 1):
        wait = max(0.0, (now - item.enqueued_at).total_seconds())
        delay = ""
        try:
            now_mono = asyncio.get_running_loop().time()
            if item.not_before_monotonic > now_mono:
                delay = f" (dans {int(item.not_before_monotonic - now_mono)}s)"
        except RuntimeError:
            pass

        lines.append(
            f"{idx}. `{item.script_key}` prio={item.priority} retry={item.retry_index} "
            f"attente={fmt_duration(wait)}{delay}"
        )
    return lines


async def enqueue_script(
    *,
    script_key: str,
    requester_id: int,
    requester_tag: str,
    channel_id: int,
    public_request: bool,
    bypass_limits: bool,
    priority: int,
    retry_index: int = 0,
    retry_of_run_id: int | None = None,
    not_before_delay_seconds: float = 0.0,
    command_args: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
    target_label: str = "",
) -> tuple[int, int]:
    if script_key not in config.SCRIPT_DEFS:
        raise ValueError(f"Script inconnu: {script_key}")

    async with config.STATE_LOCK:
        if script_key in config.RUNNING_SCRIPTS or script_key in queued_script_keys():
            raise RuntimeError(f"Le script {script_key} est deja en cours ou en file")

        config.QUEUE_SEQ += 1
        queue_id = config.QUEUE_SEQ
        now = utc_now()
        now_mono = asyncio.get_running_loop().time()

        item = config.QueuedScript(
            queue_id=queue_id,
            script_key=script_key,
            requester_id=requester_id,
            requester_tag=requester_tag,
            channel_id=channel_id,
            public_request=public_request,
            bypass_limits=bypass_limits,
            priority=max(1, min(priority, 9)),
            retry_index=max(0, retry_index),
            retry_of_run_id=retry_of_run_id,
            enqueued_at=now,
            not_before_monotonic=now_mono + max(0.0, not_before_delay_seconds),
            command_args=list(command_args or []),
            extra_env=dict(extra_env or {}),
            target_label=target_label.strip()[:120],
        )
        config.RUN_QUEUE.append(item)

        ordered = sorted(config.RUN_QUEUE, key=lambda q: (q.priority, q.enqueued_at, q.queue_id))
        position = 1 + next((idx for idx, q in enumerate(ordered) if q.queue_id == queue_id), 0)

    mark_panel_dirty()
    server_log(
        level="info",
        event="queue_enqueue",
        actor_id=requester_id,
        channel_id=channel_id,
        details=(
            f"queue_id={queue_id} script={script_key} prio={priority} retry={retry_index} "
            f"target={target_label[:80]}"
        ),
    )
    return queue_id, position


def _pick_queue_index_locked() -> int | None:
    if not config.RUN_QUEUE:
        return None

    max_parallel = get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)
    running_count = len(config.RUNNING_SCRIPTS)
    now_mono = asyncio.get_running_loop().time()

    ordered_indices = sorted(
        range(len(config.RUN_QUEUE)), key=lambda i: (config.RUN_QUEUE[i].priority, config.RUN_QUEUE[i].enqueued_at, config.RUN_QUEUE[i].queue_id)
    )

    for idx in ordered_indices:
        item = config.RUN_QUEUE[idx]
        if item.not_before_monotonic > now_mono:
            continue
        if item.script_key in config.RUNNING_SCRIPTS:
            continue
        if not item.bypass_limits and running_count >= max_parallel:
            continue
        return idx
    return None


async def launch_script(
    *,
    script_key: str,
    requester_id: int,
    requester_tag: str,
    channel_id: int,
    public_request: bool,
    bypass_limits: bool = False,
    priority: int = 5,
    queue_id: int = 0,
    retry_index: int = 0,
    command_args: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
    target_label: str = "",
) -> tuple[int, int]:
    if script_key not in config.SCRIPT_DEFS:
        raise ValueError(f"Script inconnu: {script_key}")

    script = config.SCRIPT_DEFS[script_key]
    safe_args = list(command_args or [])
    command = [*script.command, *safe_args]
    max_parallel = get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)

    async with config.STATE_LOCK:
        if script_key in config.RUNNING_SCRIPTS:
            raise RuntimeError(f"Le script {script_key} est deja en cours d'execution")
        if not bypass_limits and len(config.RUNNING_SCRIPTS) >= max_parallel:
            raise RuntimeError(f"Limite de scripts en parallele atteinte ({max_parallel})")

        ts = utc_now()
        stamp = ts.strftime("%Y%m%d_%H%M%S")
        log_path = config.RUN_LOG_DIR / f"run_{stamp}_{script_key}.log"
        run_id = insert_run(
            script_key=script_key,
            requester_id=requester_id,
            requester_tag=requester_tag,
            public_request=public_request,
            command=command,
            started_at=ts.isoformat(),
            log_path=log_path,
        )

        log_handle = log_path.open("w", encoding="utf-8")
        try:
            child_env = os.environ.copy()
            child_env["MUFFYBOT_DRY_RUN"] = "1" if dry_run_enabled() else "0"
            child_env["LUFFYBOT_RUN_ID"] = str(run_id)
            child_env["LUFFYBOT_SCRIPT_KEY"] = script_key
            child_env["LUFFYBOT_TARGET_LABEL"] = target_label.strip()[:120]
            if extra_env:
                for key, value in extra_env.items():
                    env_key = str(key).strip()
                    if not env_key:
                        continue
                    child_env[env_key] = str(value)
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(config.PYWIKIBOT_DIR),
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                env=child_env,
            )
        except Exception:
            log_handle.close()
            finalize_run(
                run_id,
                status="failed",
                return_code=None,
                note="Impossible de demarrer le processus",
                ended_at=utc_now_iso(),
                duration_seconds=0.0,
            )
            raise

        running = config.RunningScript(
            run_id=run_id,
            script_key=script_key,
            requester_id=requester_id,
            requester_tag=requester_tag,
            public_request=public_request,
            channel_id=channel_id,
            process=process,
            log_path=log_path,
            log_handle=log_handle,
            timeout_seconds=script.timeout_seconds,
            started_at=ts,
            started_monotonic=asyncio.get_running_loop().time(),
            priority=priority,
            queue_id=queue_id,
            retry_index=retry_index,
            command_args=safe_args,
            target_label=target_label.strip()[:120],
        )
        config.RUNNING_SCRIPTS[script_key] = running

    server_log(
        level="info",
        event="run_start",
        actor_id=requester_id,
        channel_id=channel_id,
        details=(
            f"run_id={run_id} queue_id={queue_id} script={script_key} pid={process.pid} "
            f"retry={retry_index} dry_run={int(dry_run_enabled())} target={target_label[:80]}"
        ),
    )
    asyncio.create_task(watch_script(running))
    return run_id, process.pid


def startup_backpressure_reason(script_key: str) -> str | None:
    script = config.SCRIPT_DEFS.get(script_key)
    if script and script.critical:
        return None

    ram_threshold = get_setting_int("startup_pressure_ram_percent", 95, min_value=50, max_value=99)
    load_threshold_x10 = get_setting_int("startup_pressure_load_per_cpu_x10", 45, min_value=5, max_value=150)
    disk_threshold = get_setting_int("startup_pressure_min_free_disk_gb", 1, min_value=0, max_value=500)

    used_mb, total_mb = memory_stats_mb()
    ram_pct = (used_mb / total_mb * 100.0) if total_mb else 0.0
    if ram_pct >= ram_threshold:
        return f"ram_pct={ram_pct:.1f}>={ram_threshold}"

    per_cpu = load_per_cpu()
    if per_cpu >= (load_threshold_x10 / 10.0):
        return f"load_per_cpu={per_cpu:.2f}>={(load_threshold_x10 / 10.0):.2f}"

    disk = shutil.disk_usage(str(config.PYWIKIBOT_DIR))
    free_gb = disk.free // (1024**3)
    if free_gb <= disk_threshold:
        return f"free_gb={free_gb}<={disk_threshold}"

    return None


async def process_queue(max_launches: int = 8) -> list[tuple[config.QueuedScript, int, int]]:
    launched: list[tuple[config.QueuedScript, int, int]] = []
    if kill_switch_enabled():
        return launched

    for _ in range(max_launches):
        async with config.STATE_LOCK:
            idx = _pick_queue_index_locked()
            if idx is None:
                break
            item = config.RUN_QUEUE.pop(idx)

        pressure = startup_backpressure_reason(item.script_key)
        if pressure and not item.bypass_limits:
            item.not_before_monotonic = asyncio.get_running_loop().time() + 8.0
            async with config.STATE_LOCK:
                if item.script_key not in config.RUNNING_SCRIPTS and item.script_key not in queued_script_keys():
                    config.RUN_QUEUE.append(item)
            server_log(
                level="warning",
                event="queue_deferred_pressure",
                actor_id=item.requester_id,
                channel_id=item.channel_id,
                details=f"queue_id={item.queue_id} script={item.script_key} reason={pressure}",
            )
            continue

        if maintenance_mode_enabled() and not item.bypass_limits:
            item.not_before_monotonic = asyncio.get_running_loop().time() + 10.0
            async with config.STATE_LOCK:
                if item.script_key not in config.RUNNING_SCRIPTS and item.script_key not in queued_script_keys():
                    config.RUN_QUEUE.append(item)
            continue

        try:
            run_id, pid = await launch_script(
                script_key=item.script_key,
                requester_id=item.requester_id,
                requester_tag=item.requester_tag,
                channel_id=item.channel_id,
                public_request=item.public_request,
                bypass_limits=item.bypass_limits,
                priority=item.priority,
                queue_id=item.queue_id,
                retry_index=item.retry_index,
                command_args=item.command_args,
                extra_env=item.extra_env,
                target_label=item.target_label,
            )
            launched.append((item, run_id, pid))
        except Exception as exc:
            if isinstance(exc, RuntimeError) and (
                "deja en cours" in str(exc).lower() or "limite" in str(exc).lower()
            ):
                item.not_before_monotonic = asyncio.get_running_loop().time() + 1.0
                async with config.STATE_LOCK:
                    if item.script_key not in config.RUNNING_SCRIPTS and item.script_key not in queued_script_keys():
                        config.RUN_QUEUE.append(item)
                break

            server_log(
                level="error",
                event="queue_launch_failed",
                actor_id=item.requester_id,
                channel_id=item.channel_id,
                details=f"queue_id={item.queue_id} script={item.script_key} err={exc}",
            )
            with contextlib.suppress(Exception):
                await critical_alert(
                    f"Echec lancement depuis queue\nqueue_id={item.queue_id} script={item.script_key}\nerr={exc}"
                )

    if launched:
        mark_panel_dirty()
        await maybe_refresh_public_panel()
        await apply_presence()
    return launched


async def request_script_start(
    *,
    script_key: str,
    requester_id: int,
    requester_tag: str,
    channel_id: int,
    public_request: bool,
    bypass_limits: bool,
    priority: int,
    retry_index: int = 0,
    retry_of_run_id: int | None = None,
    not_before_delay_seconds: float = 0.0,
    command_args: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
    target_label: str = "",
) -> dict[str, Any]:
    if kill_switch_enabled():
        raise RuntimeError("Kill switch actif: lancement bloque")
    if maintenance_mode_enabled() and public_request and not bypass_limits:
        raise RuntimeError("Mode maintenance actif: lancement public bloque")

    queue_id, position = await enqueue_script(
        script_key=script_key,
        requester_id=requester_id,
        requester_tag=requester_tag,
        channel_id=channel_id,
        public_request=public_request,
        bypass_limits=bypass_limits,
        priority=priority,
        retry_index=retry_index,
        retry_of_run_id=retry_of_run_id,
        not_before_delay_seconds=not_before_delay_seconds,
        command_args=command_args,
        extra_env=extra_env,
        target_label=target_label,
    )

    launched = await process_queue()
    for item, run_id, pid in launched:
        if item.queue_id == queue_id:
            return {
                "state": "started",
                "queue_id": queue_id,
                "run_id": run_id,
                "pid": pid,
            }

    return {
        "state": "queued",
        "queue_id": queue_id,
        "position": position,
    }


async def stop_script(script_key: str, note: str) -> bool:
    async with config.STATE_LOCK:
        running = config.RUNNING_SCRIPTS.get(script_key)
        if not running:
            return False
        config.STOP_REQUESTED.add(running.run_id)
        process = running.process

    with contextlib.suppress(ProcessLookupError):
        process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=8)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            process.kill()

    server_log(level="warning", event="run_stop_requested", details=f"script={script_key} note={note}")
    return True


async def stop_all_scripts(note: str) -> int:
    async with config.STATE_LOCK:
        scripts = sorted(config.RUNNING_SCRIPTS.keys())
    stopped = 0
    for script in scripts:
        if await stop_script(script, note=note):
            stopped += 1
    return stopped


async def run_systemd_action(action: str, service: str) -> tuple[int, str]:
    if action == "status":
        cmd = ["/usr/bin/sudo", "/bin/systemctl", "--no-pager", "--full", "status", service]
    else:
        cmd = ["/usr/bin/sudo", "/bin/systemctl", action, service]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await proc.communicate()
    output = stdout.decode("utf-8", errors="replace")
    output = redact_sensitive(output)
    return proc.returncode, output


def resource_violation_for_running(running: config.RunningScript) -> str | None:
    script = config.SCRIPT_DEFS[running.script_key]

    max_proc_mb = get_setting_int("max_process_ram_mb", 1400, min_value=64, max_value=32768)
    max_ram_pct = get_setting_int("max_system_ram_percent", 92, min_value=50, max_value=99)
    max_load_x10 = get_setting_int("max_load_per_cpu_x10", 30, min_value=5, max_value=120)
    min_free_disk_gb = get_setting_int("min_free_disk_gb", 2, min_value=0, max_value=500)

    rss_mb = process_rss_mb(running.process.pid)
    if rss_mb > max_proc_mb:
        return f"RSS process trop eleve: {rss_mb}MB > {max_proc_mb}MB"

    used_mb, total_mb = memory_stats_mb()
    ram_pct = (used_mb / total_mb * 100.0) if total_mb else 0.0
    if ram_pct > max_ram_pct and not script.critical:
        return f"RAM systeme trop elevee: {ram_pct:.1f}% > {max_ram_pct}%"

    per_cpu = load_per_cpu()
    if per_cpu > (max_load_x10 / 10.0) and not script.critical:
        return f"Charge CPU trop elevee: {per_cpu:.2f}/cpu > {(max_load_x10 / 10.0):.2f}/cpu"

    disk = shutil.disk_usage(str(config.PYWIKIBOT_DIR))
    free_gb = disk.free // (1024**3)
    if free_gb < min_free_disk_gb:
        return f"Disque libre insuffisant: {free_gb}GB < {min_free_disk_gb}GB"

    return None


def track_failure(script_key: str) -> int:
    now_mono = asyncio.get_running_loop().time()
    window = config.FAILURE_TRACK[script_key]
    window.append(now_mono)
    cutoff = now_mono - 900.0
    while window and window[0] < cutoff:
        window.popleft()
    return len(window)


async def maybe_schedule_retry(running: config.RunningScript, status: str, note: str, was_stopped: bool) -> None:
    if was_stopped:
        return
    if status not in {"failed", "timed_out", "killed_resource"}:
        return

    max_retry = get_setting_int("max_auto_retries", 1, min_value=0, max_value=5)
    if running.retry_index >= max_retry:
        return

    backoff = get_setting_int("retry_backoff_seconds", 45, min_value=5, max_value=3600)
    delay = min(backoff * (2**running.retry_index), 3600)

    try:
        result = await request_script_start(
            script_key=running.script_key,
            requester_id=running.requester_id,
            requester_tag=running.requester_tag,
            channel_id=running.channel_id,
            public_request=running.public_request,
            bypass_limits=not running.public_request,
            priority=max(1, running.priority),
            retry_index=running.retry_index + 1,
            retry_of_run_id=running.run_id,
            not_before_delay_seconds=delay,
            command_args=running.command_args,
            target_label=running.target_label,
        )
    except Exception as exc:
        await critical_alert(
            f"Retry impossible\nrun_id={running.run_id} script={running.script_key} retry={running.retry_index + 1}\nerr={exc}"
        )
        return

    server_log(
        level="warning",
        event="run_retry_scheduled",
        actor_id=running.requester_id,
        channel_id=running.channel_id,
        details=(
            f"from_run={running.run_id} script={running.script_key} retry={running.retry_index + 1} "
            f"delay={delay}s state={result['state']} note={note[:200]}"
        ),
    )


async def watch_script(running: config.RunningScript) -> None:
    status = "failed"
    note = ""
    return_code: int | None = None
    was_stopped = False

    loop = asyncio.get_running_loop()
    deadline = running.started_monotonic + max(running.timeout_seconds, 1)

    try:
        while True:
            now_mono = loop.time()
            if now_mono >= deadline:
                with contextlib.suppress(ProcessLookupError):
                    running.process.kill()
                with contextlib.suppress(Exception):
                    return_code = await running.process.wait()
                status = "timed_out"
                note = f"Timeout atteint ({running.timeout_seconds}s)"
                break

            violation = resource_violation_for_running(running)
            if violation:
                with contextlib.suppress(ProcessLookupError):
                    running.process.kill()
                with contextlib.suppress(Exception):
                    return_code = await running.process.wait()
                status = "killed_resource"
                note = violation
                break

            wait_slice = min(2.5, max(0.15, deadline - now_mono))
            try:
                return_code = await asyncio.wait_for(running.process.wait(), timeout=wait_slice)
                was_stopped = running.run_id in config.STOP_REQUESTED
                if was_stopped:
                    status = "killed"
                    note = "Arret demande par operateur"
                elif return_code == 0:
                    status = "success"
                else:
                    status = "failed"
                    note = f"Code retour non nul: {return_code}"
                break
            except asyncio.TimeoutError:
                continue
    except Exception as exc:
        status = "failed"
        note = f"Exception watcher: {exc}"
    finally:
        duration = loop.time() - running.started_monotonic
        with contextlib.suppress(Exception):
            running.log_handle.flush()
            running.log_handle.close()

        finalize_run(
            running.run_id,
            status=status,
            return_code=return_code,
            note=note,
            ended_at=utc_now_iso(),
            duration_seconds=duration,
        )

        async with config.STATE_LOCK:
            config.RUNNING_SCRIPTS.pop(running.script_key, None)
            config.STOP_REQUESTED.discard(running.run_id)

        server_log(
            level="warning" if status != "success" else "info",
            event="run_finish",
            actor_id=running.requester_id,
            channel_id=running.channel_id,
            details=(
                f"run_id={running.run_id} script={running.script_key} status={status} rc={return_code} "
                f"dur={fmt_duration(duration)} note={note[:500]}"
            ),
        )

        mark_panel_dirty()
        await process_queue()
        await maybe_refresh_public_panel()
        await apply_presence()

        if status != "success":
            failures = track_failure(running.script_key)
            msg = (
                f"Script termine en anomalie\n"
                f"run_id={running.run_id} script={running.script_key} status={status} "
                f"rc={return_code} duree={fmt_duration(duration)}\n"
                f"retry={running.retry_index} note={note}\n"
                f"log={running.log_path}"
            )
            with contextlib.suppress(Exception):
                await send_supervision(msg)

            if failures >= 3:
                with contextlib.suppress(Exception):
                    await critical_alert(
                        f"Crash loop detecte\nscript={running.script_key}\n"
                        f"erreurs_15min={failures}\ndernier_run={running.run_id}"
                    )

            if status in {"timed_out", "killed_resource"}:
                with contextlib.suppress(Exception):
                    await critical_alert(
                        f"Incident critique script\nrun_id={running.run_id} script={running.script_key} status={status}\nnote={note}"
                    )

            with contextlib.suppress(Exception):
                await maybe_schedule_retry(running, status, note, was_stopped)


def build_period_digest_message(kind: str, start: dt.datetime, end: dt.datetime) -> str:
    summary = summarize_runs(start.isoformat(), end.isoformat())
    status_txt = ", ".join([f"{name}:{count}" for name, count in summary["by_status"]]) or "aucun"
    script_txt = ", ".join([f"{name}:{count}" for name, count in summary["by_script"]]) or "aucun"
    failed_txt = ", ".join([f"{name}:{count}" for name, count in summary["by_script_failed"]]) or "aucun"

    anomalies: list[str] = []
    if summary["total"] >= 10 and summary["success_rate"] < 70.0:
        anomalies.append("success_rate_low")
    if summary["failure_count"] >= 8:
        anomalies.append("many_failures")
    if len(config.RUN_QUEUE) >= 15:
        anomalies.append("queue_high")
    anomaly_txt = ",".join(anomalies) if anomalies else "none"

    return (
        f"Digest {kind}\n"
        f"periode={start.date()} -> {(end - dt.timedelta(seconds=1)).date()}\n"
        f"runs_total={summary['total']} success={summary['success_count']} failed={summary['failure_count']} "
        f"success_rate={summary['success_rate']:.1f}% avg_dur={fmt_duration(summary['avg_duration'])}\n"
        f"etat_live=running:{len(config.RUNNING_SCRIPTS)} queue:{len(config.RUN_QUEUE)} dry_run:{int(dry_run_enabled())} anomalies:{anomaly_txt}\n"
        f"par_statut={status_txt}\n"
        f"top_scripts={script_txt}\n"
        f"top_failures={failed_txt}"
    )


async def maybe_send_periodic_digests() -> None:
    now = utc_now()

    previous_day = (now - dt.timedelta(days=1)).date()
    daily_key = previous_day.isoformat()
    if get_setting("last_daily_digest_date", "") != daily_key:
        start = dt.datetime.combine(previous_day, dt.time.min, tzinfo=dt.timezone.utc)
        end = start + dt.timedelta(days=1)
        await send_supervision(build_period_digest_message("quotidien", start, end))
        set_setting("last_daily_digest_date", daily_key)

    today = now.date()
    week_start = today - dt.timedelta(days=today.weekday())
    prev_week_start = week_start - dt.timedelta(days=7)
    prev_week_end = week_start
    iso = prev_week_start.isocalendar()
    weekly_key = f"{iso.year}-W{iso.week:02d}"
    if get_setting("last_weekly_digest_key", "") != weekly_key:
        start = dt.datetime.combine(prev_week_start, dt.time.min, tzinfo=dt.timezone.utc)
        end = dt.datetime.combine(prev_week_end, dt.time.min, tzinfo=dt.timezone.utc)
        await send_supervision(build_period_digest_message("hebdomadaire", start, end))
        set_setting("last_weekly_digest_key", weekly_key)

    first_of_this_month = now.date().replace(day=1)
    last_of_prev_month = first_of_this_month - dt.timedelta(days=1)
    first_of_prev_month = last_of_prev_month.replace(day=1)
    monthly_key = f"{first_of_prev_month.year}-{first_of_prev_month.month:02d}"
    if get_setting("last_monthly_digest_key", "") != monthly_key:
        start = dt.datetime.combine(first_of_prev_month, dt.time.min, tzinfo=dt.timezone.utc)
        end = dt.datetime.combine(first_of_this_month, dt.time.min, tzinfo=dt.timezone.utc)
        await send_supervision(build_period_digest_message("mensuel", start, end))
        set_setting("last_monthly_digest_key", monthly_key)


async def maybe_run_daily_ops() -> None:
    today = utc_now().date().isoformat()

    if get_setting("last_daily_bot_logs_date", "") != today:
        try:
            await request_script_start(
                script_key="daily-bot-logs",
                requester_id=config.OWNER_USER_ID,
                requester_tag="system",
                channel_id=get_setting_int("digest_channel_id", config.SUPERVISION_CHANNEL_ID, min_value=1, max_value=10**20),
                public_request=False,
                bypass_limits=True,
                priority=1,
            )
            set_setting("last_daily_bot_logs_date", today)
        except Exception as exc:
            with contextlib.suppress(Exception):
                await send_supervision(f"Daily bot logs non lance: {exc}")

    if get_setting("last_daily_config_backup_date", "") != today:
        try:
            await request_script_start(
                script_key="config-backup",
                requester_id=config.OWNER_USER_ID,
                requester_tag="system",
                channel_id=get_setting_int("digest_channel_id", config.SUPERVISION_CHANNEL_ID, min_value=1, max_value=10**20),
                public_request=False,
                bypass_limits=True,
                priority=1,
            )
            set_setting("last_daily_config_backup_date", today)
        except Exception as exc:
            with contextlib.suppress(Exception):
                await send_supervision(f"Backup config quotidien non lance: {exc}")


def purge_old_files(directory: Path, retention_days: int) -> int:
    if retention_days <= 0 or not directory.exists():
        return 0

    cutoff = utc_now() - dt.timedelta(days=retention_days)
    removed = 0
    for path in directory.glob("*"):
        if not path.is_file():
            continue
        with contextlib.suppress(Exception):
            mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
            if mtime < cutoff:
                path.unlink(missing_ok=True)
                removed += 1
    return removed


def export_runs_csv_file(days: int = 30) -> Path:
    days = max(min(days, 365), 1)
    start = utc_now() - dt.timedelta(days=days)
    start_iso = start.isoformat()

    out_path = config.RUN_LOG_DIR / f"runs_export_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
    with db_connect() as conn, out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "id",
                "script_key",
                "requester_id",
                "requester_tag",
                "public_request",
                "status",
                "return_code",
                "started_at",
                "ended_at",
                "duration_seconds",
                "log_path",
                "note",
            ]
        )

        rows = conn.execute(
            """
            SELECT id, script_key, requester_id, requester_tag, public_request,
                   status, return_code, started_at, ended_at, duration_seconds, log_path, note
            FROM script_runs
            WHERE started_at >= ?
            ORDER BY id DESC
            LIMIT 5000
            """,
            (start_iso,),
        ).fetchall()

        for row in rows:
            writer.writerow(
                [
                    row["id"],
                    row["script_key"],
                    row["requester_id"],
                    row["requester_tag"],
                    row["public_request"],
                    row["status"],
                    row["return_code"],
                    row["started_at"],
                    row["ended_at"],
                    row["duration_seconds"],
                    row["log_path"],
                    redact_sensitive(str(row["note"] or "")),
                ]
            )
    return out_path


def backup_database() -> Path:
    stamp = utc_now().strftime("%Y%m%d_%H%M%S")
    backup_path = config.DB_BACKUP_DIR / f"luffybot_{stamp}.sqlite3"
    shutil.copy2(config.DB_PATH, backup_path)
    return backup_path


def latest_database_backup() -> Path | None:
    backups = sorted(config.DB_BACKUP_DIR.glob("luffybot_*.sqlite3"))
    if not backups:
        return None
    return backups[-1]


def restore_latest_backup() -> Path:
    backup = latest_database_backup()
    if not backup:
        raise RuntimeError("Aucun backup disponible")
    shutil.copy2(backup, config.DB_PATH)
    return backup


async def queue_worker_loop() -> None:
    while True:
        try:
            await process_queue(max_launches=12)
            await maybe_refresh_public_panel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            config.LOGGER.exception("queue_worker_loop error: %s", exc)
            with contextlib.suppress(Exception):
                await critical_alert(f"queue_worker_loop error: {exc}")
        await asyncio.sleep(0.25)


async def housekeeping_loop() -> None:
    last_cleanup = 0.0
    last_presence = 0.0
    last_digest = 0.0

    while True:
        now_mono = asyncio.get_running_loop().time()

        try:
            if kill_switch_enabled() and config.RUNNING_SCRIPTS:
                await stop_all_scripts("Kill switch actif (housekeeping)")

            if now_mono - last_presence >= 8.0:
                await apply_presence()
                last_presence = now_mono

            if now_mono - last_digest >= 60.0:
                await maybe_send_periodic_digests()
                await maybe_run_daily_ops()
                last_digest = now_mono

            if now_mono - last_cleanup >= 3600.0:
                retention = get_setting_int("log_retention_days", 14, min_value=1, max_value=365)
                removed_logs = purge_old_files(config.RUN_LOG_DIR, retention)
                removed_backups = purge_old_files(config.DB_BACKUP_DIR, retention * 4)
                server_log(
                    level="info",
                    event="housekeeping_cleanup",
                    details=f"removed_logs={removed_logs} removed_backups={removed_backups}",
                )
                last_cleanup = now_mono

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            config.LOGGER.exception("housekeeping_loop error: %s", exc)
            with contextlib.suppress(Exception):
                await critical_alert(f"housekeeping_loop error: {exc}")

        await asyncio.sleep(1.0)


def ensure_background_tasks_started() -> None:
    sync_control_files()
    if "queue" not in config.BACKGROUND_TASKS or config.BACKGROUND_TASKS["queue"].done():
        config.BACKGROUND_TASKS["queue"] = asyncio.create_task(queue_worker_loop(), name="queue_worker")

    if "housekeeping" not in config.BACKGROUND_TASKS or config.BACKGROUND_TASKS["housekeeping"].done():
        config.BACKGROUND_TASKS["housekeeping"] = asyncio.create_task(housekeeping_loop(), name="housekeeping")


def build_health_embed(title: str = "Sante") -> discord.Embed:
    used_mb, total_mb = memory_stats_mb()
    disk = shutil.disk_usage(str(config.PYWIKIBOT_DIR))
    free_gb = disk.free // (1024**3)
    used_pct = memory_used_percent()
    per_cpu = load_per_cpu()

    db_ok = True
    db_msg = "ok"
    try:
        with db_connect() as conn:
            conn.execute("SELECT 1").fetchone()
    except Exception as exc:
        db_ok = False
        db_msg = str(exc)

    embed = discord.Embed(title=title, color=0x2ECC71 if db_ok else 0xE74C3C, timestamp=utc_now())
    embed.add_field(name="DB", value=f"{'OK' if db_ok else 'ERREUR'} ({db_msg[:120]})", inline=False)
    embed.add_field(name="RAM", value=f"{used_mb}/{total_mb} MB ({used_pct:.1f}%)", inline=True)
    embed.add_field(name="Disque libre", value=f"{free_gb} GB", inline=True)
    embed.add_field(name="Load/cpu", value=f"{per_cpu:.2f}", inline=True)
    embed.add_field(name="Queue", value=str(len(config.RUN_QUEUE)), inline=True)
    embed.add_field(name="Running", value=str(len(config.RUNNING_SCRIPTS)), inline=True)
    embed.add_field(name="Latency", value=f"{int(config.bot.latency * 1000)} ms", inline=True)
    return embed
