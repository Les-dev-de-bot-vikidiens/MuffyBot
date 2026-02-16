#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import discord

LOGGER = logging.getLogger("luffybot")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "luffybot.sqlite3"
RUN_LOG_DIR = BASE_DIR / "run_logs"
TOKEN_FILE = BASE_DIR / "token.txt"

OWNER_USER_ID = int(os.getenv("OWNER_USER_ID", "1424064908244422668"))
SUPERVISION_CHANNEL_ID = int(os.getenv("SUPERVISION_CHANNEL_ID", "1427596219676495904"))
PYWIKIBOT_DIR = Path(os.getenv("PYWIKIBOT_DIR", "/home/ubuntu/pywikibot-scripts"))
PYTHON_BIN = os.getenv("PYTHON_BIN", "/usr/bin/python3")

ALLOWED_SYSTEMD_SERVICES = [
    "luffybot.service",
    "logs.service",
    "vandalism.service",
    "muffy_dashboard.service",
    "certifhub.service",
]


@dataclass(frozen=True)
class ScriptDef:
    key: str
    command: list[str]
    timeout_seconds: int
    public: bool
    description: str


SCRIPT_DEFS: dict[str, ScriptDef] = {
    "vandalism-fr": ScriptDef("vandalism-fr", [PYTHON_BIN, "vandalism.py"], 240, True, "Anti-vandalisme FR"),
    "vandalism-en": ScriptDef("vandalism-en", [PYTHON_BIN, "envikidia/vandalism.py"], 240, True, "Anti-vandalisme EN"),
    "welcome": ScriptDef("welcome", [PYTHON_BIN, "welcome.py"], 540, True, "Messages de bienvenue"),
    "homonym": ScriptDef("homonym", [PYTHON_BIN, "homonym.py"], 3300, True, "Nettoyage homonymies"),
    "categinex": ScriptDef("categinex", [PYTHON_BIN, "categinex.py"], 7200, True, "Nettoyage catégories"),
    "sandboxreset-en": ScriptDef("sandboxreset-en", [PYTHON_BIN, "envikidia/sandboxreset.py"], 150, True, "Reset bac à sable EN"),
    "weekly-talk-en": ScriptDef("weekly-talk-en", [PYTHON_BIN, "envikidia/semaine.py"], 7200, True, "Discussion hebdo EN"),
    "annual-pages-en": ScriptDef("annual-pages-en", [PYTHON_BIN, "envikidia/main.py"], 7200, False, "Création pages annuelles EN"),
    "daily-report": ScriptDef("daily-report", [PYTHON_BIN, "daily_report.py"], 1200, True, "Rapport quotidien"),
    "weekly-report": ScriptDef("weekly-report", [PYTHON_BIN, "weekly_report.py"], 1800, True, "Rapport hebdomadaire"),
    "monthly-report": ScriptDef("monthly-report", [PYTHON_BIN, "monthly_report.py"], 2700, False, "Rapport mensuel"),
    "doctor": ScriptDef("doctor", [PYTHON_BIN, "doctor.py"], 900, True, "Diagnostic de santé"),
}

PUBLIC_SCRIPT_CHOICES = sorted([name for name, spec in SCRIPT_DEFS.items() if spec.public])
ALL_SCRIPT_CHOICES = sorted(SCRIPT_DEFS.keys())


@dataclass
class RunningScript:
    run_id: int
    script_key: str
    requester_id: int
    requester_tag: str
    public_request: bool
    channel_id: int
    process: asyncio.subprocess.Process
    log_path: Path
    log_handle: Any
    timeout_seconds: int
    started_at: dt.datetime
    started_monotonic: float


RUNNING_SCRIPTS: dict[str, RunningScript] = {}
STOP_REQUESTED: set[int] = set()
LAST_PUBLIC_START_MONO: dict[int, float] = {}
STATE_LOCK = asyncio.Lock()


intents = discord.Intents.default()
intents.guilds = True
intents.members = True
bot = discord.Bot(intents=intents)


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    total = int(max(seconds, 0))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def memory_stats_mb() -> tuple[int, int]:
    total_kib = 0
    available_kib = 0
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    total_kib = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    available_kib = int(line.split()[1])
    except Exception:
        return 0, 0
    used_kib = max(total_kib - available_kib, 0)
    return used_kib // 1024, total_kib // 1024


def redact_sensitive(text: str) -> str:
    redacted = text
    redacted = redacted.replace("https://discord.com/api/webhooks/", "https://discord.com/api/webhooks/[REDACTED]/")
    redacted = redacted.replace("https://ptb.discord.com/api/webhooks/", "https://ptb.discord.com/api/webhooks/[REDACTED]/")
    redacted = redacted.replace("https://canary.discord.com/api/webhooks/", "https://canary.discord.com/api/webhooks/[REDACTED]/")
    for marker in ("TOKEN=", "DISCORD_TOKEN=", "MISTRAL_API_KEY="):
        if marker in redacted:
            redacted = redacted.replace(marker, f"{marker}[REDACTED]")
    return redacted


def read_tail(path: Path, lines: int = 80, max_chars: int = 3500) -> str:
    if not path.exists():
        return "Log introuvable."
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return f"Impossible de lire le log: {exc}"
    snippet = "\n".join(content.splitlines()[-max(lines, 1) :])
    snippet = redact_sensitive(snippet)
    return snippet[-max_chars:]


def load_token() -> str:
    token = (os.getenv("DISCORD_TOKEN") or "").strip()
    if token:
        return token
    if TOKEN_FILE.exists():
        return TOKEN_FILE.read_text(encoding="utf-8").strip()
    raise RuntimeError("DISCORD_TOKEN manquant (env ou token.txt)")


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS script_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                script_key TEXT NOT NULL,
                requester_id INTEGER NOT NULL,
                requester_tag TEXT NOT NULL,
                public_request INTEGER NOT NULL,
                command_json TEXT NOT NULL,
                status TEXT NOT NULL,
                return_code INTEGER,
                note TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_seconds REAL,
                log_path TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS op_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                actor_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                target TEXT,
                details TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_script_runs_script ON script_runs(script_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_script_runs_started ON script_runs(started_at)")

        defaults = {
            "maintenance_mode": "0",
            "public_start_enabled": "1",
            "max_parallel_runs": "4",
            "public_cooldown_seconds": "120",
            "public_panel_channel_id": "",
            "public_panel_message_id": "",
        }
        for key, value in defaults.items():
            conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (key, value))


def get_setting(key: str, default: str) -> str:
    with db_connect() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    return str(row["value"])


def set_setting(key: str, value: str) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO settings(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def get_setting_bool(key: str, default: bool) -> bool:
    raw = get_setting(key, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def get_setting_int(key: str, default: int, min_value: int = 1, max_value: int = 10000) -> int:
    raw = get_setting(key, str(default))
    try:
        value = int(raw)
    except ValueError:
        value = default
    return max(min(value, max_value), min_value)


def get_setting_int_optional(key: str) -> int | None:
    raw = get_setting(key, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def set_public_panel_location(channel_id: int, message_id: int) -> None:
    set_setting("public_panel_channel_id", str(channel_id))
    set_setting("public_panel_message_id", str(message_id))


def get_public_panel_location() -> tuple[int | None, int | None]:
    return get_setting_int_optional("public_panel_channel_id"), get_setting_int_optional("public_panel_message_id")


def clear_public_panel_location() -> None:
    set_setting("public_panel_channel_id", "")
    set_setting("public_panel_message_id", "")


def insert_run(
    *,
    script_key: str,
    requester_id: int,
    requester_tag: str,
    public_request: bool,
    command: list[str],
    started_at: str,
    log_path: Path,
) -> int:
    with db_connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO script_runs(
                script_key, requester_id, requester_tag, public_request,
                command_json, status, started_at, log_path
            ) VALUES(?, ?, ?, ?, ?, 'running', ?, ?)
            """,
            (
                script_key,
                requester_id,
                requester_tag,
                1 if public_request else 0,
                json.dumps(command, ensure_ascii=False),
                started_at,
                str(log_path),
            ),
        )
        return int(cursor.lastrowid)


def finalize_run(
    run_id: int,
    *,
    status: str,
    return_code: int | None,
    note: str,
    ended_at: str,
    duration_seconds: float,
) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            UPDATE script_runs
            SET status = ?, return_code = ?, note = ?, ended_at = ?, duration_seconds = ?
            WHERE id = ?
            """,
            (status, return_code, note[:2000], ended_at, duration_seconds, run_id),
        )


def last_runs(script_key: str | None = None, limit: int = 10) -> list[sqlite3.Row]:
    limit = max(min(limit, 50), 1)
    with db_connect() as conn:
        if script_key:
            rows = conn.execute(
                """
                SELECT * FROM script_runs
                WHERE script_key = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (script_key, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM script_runs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    return list(rows)


def get_run_by_id(run_id: int) -> sqlite3.Row | None:
    with db_connect() as conn:
        row = conn.execute("SELECT * FROM script_runs WHERE id = ?", (run_id,)).fetchone()
    return row


def audit(actor_id: int, action: str, target: str = "", details: str = "") -> None:
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO op_audit(ts, actor_id, action, target, details) VALUES(?, ?, ?, ?, ?)",
            (utc_now_iso(), actor_id, action[:200], target[:200], details[:2000]),
        )


async def supervision_channel() -> discord.abc.Messageable | None:
    channel = bot.get_channel(SUPERVISION_CHANNEL_ID)
    if channel:
        return channel
    try:
        return await bot.fetch_channel(SUPERVISION_CHANNEL_ID)
    except Exception:
        return None


async def send_supervision(message: str) -> None:
    channel = await supervision_channel()
    if not channel:
        LOGGER.warning("Supervision channel unavailable")
        return
    safe = redact_sensitive(message)
    if len(safe) <= 1900:
        await channel.send(f"```\n{safe}\n```")
        return
    path = RUN_LOG_DIR / f"supervision_{int(utc_now().timestamp())}.txt"
    path.write_text(safe, encoding="utf-8")
    await channel.send("Supervision log:", file=discord.File(str(path), filename=path.name))


def is_owner(user_id: int) -> bool:
    return int(user_id) == OWNER_USER_ID


async def ensure_owner_ctx(ctx: discord.ApplicationContext) -> bool:
    if is_owner(ctx.author.id):
        return True
    await ctx.respond("Commande réservée au propriétaire.", ephemeral=True)
    return False


async def ensure_owner_interaction(interaction: discord.Interaction) -> bool:
    if is_owner(interaction.user.id):
        return True
    await respond_ephemeral(interaction, "Action réservée au propriétaire.")
    return False


async def respond_ephemeral(
    interaction: discord.Interaction,
    content: str | None = None,
    *,
    file: discord.File | None = None,
    embed: discord.Embed | None = None,
) -> None:
    if interaction.response.is_done():
        await interaction.followup.send(content=content, ephemeral=True, file=file, embed=embed)
    else:
        await interaction.response.send_message(content=content, ephemeral=True, file=file, embed=embed)


async def launch_script(
    *,
    script_key: str,
    requester_id: int,
    requester_tag: str,
    channel_id: int,
    public_request: bool,
    bypass_limits: bool = False,
) -> tuple[int, int]:
    if script_key not in SCRIPT_DEFS:
        raise ValueError(f"Script inconnu: {script_key}")
    script = SCRIPT_DEFS[script_key]

    max_parallel = get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)

    async with STATE_LOCK:
        if script_key in RUNNING_SCRIPTS:
            raise RuntimeError(f"Le script {script_key} est déjà en cours d'exécution")
        if not bypass_limits and len(RUNNING_SCRIPTS) >= max_parallel:
            raise RuntimeError(f"Limite de scripts en parallèle atteinte ({max_parallel})")

        ts = utc_now()
        stamp = ts.strftime("%Y%m%d_%H%M%S")
        log_path = RUN_LOG_DIR / f"run_{stamp}_{script_key}.log"
        run_id = insert_run(
            script_key=script_key,
            requester_id=requester_id,
            requester_tag=requester_tag,
            public_request=public_request,
            command=script.command,
            started_at=ts.isoformat(),
            log_path=log_path,
        )

        log_handle = log_path.open("w", encoding="utf-8")
        try:
            process = await asyncio.create_subprocess_exec(
                *script.command,
                cwd=str(PYWIKIBOT_DIR),
                stdout=log_handle,
                stderr=subprocess.STDOUT,
            )
        except Exception:
            log_handle.close()
            finalize_run(
                run_id,
                status="failed",
                return_code=None,
                note="Impossible de démarrer le processus",
                ended_at=utc_now_iso(),
                duration_seconds=0.0,
            )
            raise

        running = RunningScript(
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
        )
        RUNNING_SCRIPTS[script_key] = running

    asyncio.create_task(watch_script(running))
    return run_id, process.pid


async def stop_script(script_key: str, note: str) -> bool:
    async with STATE_LOCK:
        running = RUNNING_SCRIPTS.get(script_key)
        if not running:
            return False
        STOP_REQUESTED.add(running.run_id)
        process = running.process
    with contextlib.suppress(ProcessLookupError):
        process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=8)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
    return True


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


async def watch_script(running: RunningScript) -> None:
    status = "failed"
    note = ""
    return_code: int | None = None
    try:
        try:
            return_code = await asyncio.wait_for(running.process.wait(), timeout=running.timeout_seconds)
            if running.run_id in STOP_REQUESTED:
                status = "killed"
                note = "Arrêt demandé par opérateur"
            elif return_code == 0:
                status = "success"
            else:
                status = "failed"
                note = f"Code retour non nul: {return_code}"
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                running.process.kill()
            with contextlib.suppress(Exception):
                return_code = await running.process.wait()
            status = "timed_out"
            note = f"Timeout atteint ({running.timeout_seconds}s)"
    except Exception as exc:
        status = "failed"
        note = f"Exception watcher: {exc}"
    finally:
        duration = asyncio.get_running_loop().time() - running.started_monotonic
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

        async with STATE_LOCK:
            RUNNING_SCRIPTS.pop(running.script_key, None)
            STOP_REQUESTED.discard(running.run_id)

        await refresh_saved_public_panel()

        if status != "success":
            msg = (
                f"<@{OWNER_USER_ID}> Script terminé en anomalie\n"
                f"run_id={running.run_id} script={running.script_key} status={status} "
                f"rc={return_code} duree={fmt_duration(duration)}\n"
                f"note={note}\n"
                f"log={running.log_path}"
            )
            with contextlib.suppress(Exception):
                await send_supervision(msg)


def build_public_panel_embed() -> discord.Embed:
    maintenance = get_setting_bool("maintenance_mode", False)
    public_enabled = get_setting_bool("public_start_enabled", True)
    max_parallel = get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)
    cooldown = get_setting_int("public_cooldown_seconds", 120, min_value=0, max_value=3600)
    used_mb, total_mb = memory_stats_mb()
    disk = shutil.disk_usage(str(PYWIKIBOT_DIR))

    lines_running: list[str] = []
    for key, running in sorted(RUNNING_SCRIPTS.items(), key=lambda item: item[1].run_id):
        elapsed = (utc_now() - running.started_at).total_seconds()
        lines_running.append(f"`{key}` pid={running.process.pid} {fmt_duration(elapsed)}")
    running_text = "\n".join(lines_running[:10]) if lines_running else "Aucun script en cours."

    rows = last_runs(None, limit=8)
    run_lines = [
        f"#{row['id']} {row['script_key']} {row['status']} rc={row['return_code']} dur={fmt_duration(row['duration_seconds'])}"
        for row in rows
    ]
    history_text = "\n".join(run_lines) if run_lines else "Aucun historique."

    embed = discord.Embed(title="Panneau Public Scripts", color=0x3498DB, timestamp=utc_now())
    embed.description = (
        f"Mode maintenance: `{'ON' if maintenance else 'OFF'}` | "
        f"Start public: `{'ON' if public_enabled else 'OFF'}`\n"
        f"Parallel: `{len(RUNNING_SCRIPTS)}/{max_parallel}` | Cooldown: `{cooldown}s`"
    )
    embed.add_field(
        name="Ressources",
        value=f"RAM `{used_mb}/{total_mb} MB` | Disque `{disk.used // (1024**3)}/{disk.total // (1024**3)} GB`",
        inline=False,
    )
    embed.add_field(name="Scripts en cours", value=running_text[:1024], inline=False)
    embed.add_field(name="Derniers runs", value=history_text[:1024], inline=False)
    return embed


def build_op_panel_embed() -> discord.Embed:
    maintenance = get_setting_bool("maintenance_mode", False)
    public_enabled = get_setting_bool("public_start_enabled", True)
    max_parallel = get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)
    cooldown = get_setting_int("public_cooldown_seconds", 120, min_value=0, max_value=3600)

    embed = discord.Embed(title="Panneau OP (éphémère)", color=0xE67E22, timestamp=utc_now())
    embed.description = (
        f"Owner ID: `{OWNER_USER_ID}`\n"
        f"Maintenance: `{'ON' if maintenance else 'OFF'}` | Public start: `{'ON' if public_enabled else 'OFF'}`\n"
        f"Parallel: `{len(RUNNING_SCRIPTS)}/{max_parallel}` | Cooldown public: `{cooldown}s`"
    )
    return embed


class PublicStartSelect(discord.ui.Select):
    def __init__(self) -> None:
        options = [
            discord.SelectOption(label=key, value=key, description=SCRIPT_DEFS[key].description[:90])
            for key in PUBLIC_SCRIPT_CHOICES
        ]
        super().__init__(
            placeholder="Lancer un script public",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=0,
            custom_id="public_panel_start_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        script = self.values[0]
        if get_setting_bool("maintenance_mode", False):
            await respond_ephemeral(interaction, "Mode maintenance actif: démarrage public désactivé.")
            return
        if not get_setting_bool("public_start_enabled", True):
            await respond_ephemeral(interaction, "Le démarrage public est actuellement désactivé.")
            return

        cooldown = get_setting_int("public_cooldown_seconds", 120, min_value=0, max_value=3600)
        now_mono = asyncio.get_running_loop().time()
        previous = LAST_PUBLIC_START_MONO.get(interaction.user.id)
        if previous is not None and now_mono - previous < cooldown:
            remain = int(cooldown - (now_mono - previous))
            await respond_ephemeral(interaction, f"Cooldown actif: réessaie dans {remain}s.")
            return

        try:
            run_id, pid = await launch_script(
                script_key=script,
                requester_id=interaction.user.id,
                requester_tag=str(interaction.user),
                channel_id=interaction.channel_id,
                public_request=True,
                bypass_limits=False,
            )
        except Exception as exc:
            await respond_ephemeral(interaction, f"Impossible de lancer `{script}`: {exc}")
            return

        LAST_PUBLIC_START_MONO[interaction.user.id] = now_mono
        await respond_ephemeral(interaction, f"Script `{script}` lancé. run_id=`{run_id}` pid=`{pid}`")
        await refresh_saved_public_panel()


class PublicStatusSelect(discord.ui.Select):
    def __init__(self) -> None:
        options = [discord.SelectOption(label=key, value=key) for key in PUBLIC_SCRIPT_CHOICES]
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
        running = RUNNING_SCRIPTS.get(script)
        last = last_runs(script, limit=1)

        embed = discord.Embed(title=f"Statut script: {script}", color=0x5865F2)
        if running:
            elapsed = (utc_now() - running.started_at).total_seconds()
            embed.add_field(name="En cours", value=f"Oui (pid {running.process.pid})", inline=True)
            embed.add_field(name="Depuis", value=fmt_duration(elapsed), inline=True)
            embed.add_field(name="run_id", value=str(running.run_id), inline=True)
        else:
            embed.add_field(name="En cours", value="Non", inline=True)

        if last:
            row = last[0]
            embed.add_field(name="Dernier statut", value=row["status"], inline=True)
            embed.add_field(name="Dernier run_id", value=str(row["id"]), inline=True)
            embed.add_field(name="Durée", value=fmt_duration(row["duration_seconds"]), inline=True)
        else:
            embed.add_field(name="Historique", value="Aucun run enregistré", inline=False)

        await respond_ephemeral(interaction, embed=embed)


class PublicLogSelect(discord.ui.Select):
    def __init__(self) -> None:
        options = [discord.SelectOption(label=key, value=key) for key in PUBLIC_SCRIPT_CHOICES]
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
            await respond_ephemeral(interaction, "Aucun run trouvé pour ce script.")
            return

        row = rows[0]
        path = Path(str(row["log_path"]))
        tail = read_tail(path, lines=100, max_chars=3500)
        header = f"run_id=#{row['id']} script={row['script_key']} status={row['status']}"
        payload = f"{header}\n{tail}".strip()
        if len(payload) <= 1800:
            await respond_ephemeral(interaction, f"```\n{payload}\n```")
            return

        tmp = RUN_LOG_DIR / f"tail_{row['id']}.txt"
        tmp.write_text(payload, encoding="utf-8")
        await respond_ephemeral(interaction, "Log en pièce jointe:", file=discord.File(str(tmp), filename=tmp.name))


class PublicPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)
        self.add_item(PublicStartSelect())
        self.add_item(PublicStatusSelect())
        self.add_item(PublicLogSelect())

    @discord.ui.button(label="Rafraîchir", style=discord.ButtonStyle.secondary, row=3, custom_id="public_panel_refresh")
    async def refresh_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        await refresh_saved_public_panel()
        await respond_ephemeral(interaction, "Panneau public rafraîchi.")

    @discord.ui.button(label="Health", style=discord.ButtonStyle.secondary, row=3, custom_id="public_panel_health")
    async def health_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        used_mb, total_mb = memory_stats_mb()
        disk = shutil.disk_usage(str(PYWIKIBOT_DIR))
        running = len(RUNNING_SCRIPTS)
        max_parallel = get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)
        maintenance = get_setting_bool("maintenance_mode", False)
        public_enabled = get_setting_bool("public_start_enabled", True)

        embed = discord.Embed(title="Santé", color=0x2ECC71)
        embed.add_field(name="RAM", value=f"{used_mb}/{total_mb} MB", inline=True)
        embed.add_field(name="Disque", value=f"{disk.used // (1024**3)}/{disk.total // (1024**3)} GB", inline=True)
        embed.add_field(name="Scripts", value=f"{running}/{max_parallel}", inline=True)
        embed.add_field(name="Maintenance", value="ON" if maintenance else "OFF", inline=True)
        embed.add_field(name="Start public", value="ON" if public_enabled else "OFF", inline=True)
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


class OpStartSelect(discord.ui.Select):
    def __init__(self) -> None:
        options = [
            discord.SelectOption(label=key, value=key, description=SCRIPT_DEFS[key].description[:90])
            for key in ALL_SCRIPT_CHOICES
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
            run_id, pid = await launch_script(
                script_key=script,
                requester_id=interaction.user.id,
                requester_tag=str(interaction.user),
                channel_id=interaction.channel_id,
                public_request=False,
                bypass_limits=True,
            )
            audit(interaction.user.id, "panel_op_start", script, f"run_id={run_id} pid={pid}")
            await respond_ephemeral(interaction, f"Lancé: `{script}` run_id=`{run_id}` pid=`{pid}`")
        except Exception as exc:
            audit(interaction.user.id, "panel_op_start_failed", script, str(exc))
            await respond_ephemeral(interaction, f"Échec lancement `{script}`: {exc}")
        await refresh_saved_public_panel()


class OpStopSelect(discord.ui.Select):
    def __init__(self) -> None:
        running_keys = sorted(RUNNING_SCRIPTS.keys())
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
            await respond_ephemeral(interaction, "Aucun script à arrêter.")
            return
        ok = await stop_script(script, note="Arrêt demandé depuis panneau OP")
        if ok:
            audit(interaction.user.id, "panel_op_stop", script, "requested")
            await respond_ephemeral(interaction, f"Stop demandé pour `{script}`")
        else:
            await respond_ephemeral(interaction, f"Script `{script}` introuvable en cours.")
        await refresh_saved_public_panel()


class OpServiceSelect(discord.ui.Select):
    def __init__(self) -> None:
        options: list[discord.SelectOption] = []
        for service in ALLOWED_SYSTEMD_SERVICES:
            for action in ("status", "restart", "start", "stop"):
                options.append(discord.SelectOption(label=f"{action} {service}", value=f"{action}|{service}"))
        super().__init__(
            placeholder="OP Service: action systemd",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=2,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        action, service = self.values[0].split("|", 1)

        await interaction.response.defer(ephemeral=True)
        rc, output = await run_systemd_action(action, service)
        audit(interaction.user.id, "panel_op_service", service, f"action={action} rc={rc}")

        summary = f"systemctl {action} {service} -> rc={rc}"
        if len(output) > 1600:
            out_file = RUN_LOG_DIR / f"systemctl_{service}_{action}_{int(utc_now().timestamp())}.txt"
            out_file.write_text(output, encoding="utf-8")
            await interaction.followup.send(summary, ephemeral=True, file=discord.File(str(out_file), filename=out_file.name))
        else:
            await interaction.followup.send(f"{summary}\n```\n{output[-1500:]}\n```", ephemeral=True)


class MaxParallelModal(discord.ui.Modal):
    def __init__(self) -> None:
        super().__init__(title="Définir max_parallel_runs")
        self.value = discord.ui.InputText(
            label="max_parallel_runs (1-20)",
            placeholder="Ex: 4",
            required=True,
            min_length=1,
            max_length=2,
            value=str(get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)),
        )
        self.add_item(self.value)

    async def callback(self, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        try:
            parsed = int(str(self.value.value).strip())
        except ValueError:
            await respond_ephemeral(interaction, "Valeur invalide.")
            return
        parsed = max(min(parsed, 20), 1)
        set_setting("max_parallel_runs", str(parsed))
        audit(interaction.user.id, "panel_op_set_max_parallel", "max_parallel_runs", str(parsed))
        await respond_ephemeral(interaction, f"max_parallel_runs={parsed}")
        await refresh_saved_public_panel()


class OpPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=900)
        self.add_item(OpStartSelect())
        self.add_item(OpStopSelect())
        self.add_item(OpServiceSelect())

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.secondary, row=3)
    async def settings_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        maintenance = get_setting_bool("maintenance_mode", False)
        public_enabled = get_setting_bool("public_start_enabled", True)
        max_parallel = get_setting_int("max_parallel_runs", 4, min_value=1, max_value=20)
        cooldown = get_setting_int("public_cooldown_seconds", 120, min_value=0, max_value=3600)
        await respond_ephemeral(
            interaction,
            "\n".join(
                [
                    f"maintenance_mode={int(maintenance)}",
                    f"public_start_enabled={int(public_enabled)}",
                    f"max_parallel_runs={max_parallel}",
                    f"public_cooldown_seconds={cooldown}",
                    f"running={len(RUNNING_SCRIPTS)}",
                ]
            ),
        )

    @discord.ui.button(label="Runs", style=discord.ButtonStyle.secondary, row=3)
    async def runs_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        rows = last_runs(None, limit=15)
        if not rows:
            await respond_ephemeral(interaction, "Aucun run.")
            return
        lines = [
            f"#{row['id']} {row['script_key']} {row['status']} rc={row['return_code']} dur={fmt_duration(row['duration_seconds'])}"
            for row in rows
        ]
        await respond_ephemeral(interaction, "\n".join(lines[:25]))

    @discord.ui.button(label="Stop All", style=discord.ButtonStyle.danger, row=3)
    async def stop_all_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        async with STATE_LOCK:
            keys = list(RUNNING_SCRIPTS.keys())
        stopped = 0
        for key in keys:
            if await stop_script(key, note="Stop all demandé depuis panneau OP"):
                stopped += 1

        audit(interaction.user.id, "panel_op_stop_all", "*", f"stopped={stopped}")
        await interaction.followup.send(f"Stop demandé pour {stopped} script(s).", ephemeral=True)
        await refresh_saved_public_panel()

    @discord.ui.button(label="Restart Bot", style=discord.ButtonStyle.danger, row=3)
    async def restart_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        audit(interaction.user.id, "panel_op_restart_bot", "luffybot", "requested")
        await respond_ephemeral(interaction, "Redémarrage du bot...")
        await asyncio.sleep(1)
        await bot.close()
        os._exit(0)

    @discord.ui.button(label="Toggle Maintenance", style=discord.ButtonStyle.primary, row=4)
    async def toggle_maintenance_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        enabled = not get_setting_bool("maintenance_mode", False)
        set_setting("maintenance_mode", "1" if enabled else "0")
        audit(interaction.user.id, "panel_op_set_maintenance", "maintenance_mode", str(int(enabled)))
        await respond_ephemeral(interaction, f"maintenance_mode={'ON' if enabled else 'OFF'}")
        await refresh_saved_public_panel()

    @discord.ui.button(label="Toggle Public Start", style=discord.ButtonStyle.primary, row=4)
    async def toggle_public_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        enabled = not get_setting_bool("public_start_enabled", True)
        set_setting("public_start_enabled", "1" if enabled else "0")
        audit(interaction.user.id, "panel_op_set_public_start", "public_start_enabled", str(int(enabled)))
        await respond_ephemeral(interaction, f"public_start_enabled={'ON' if enabled else 'OFF'}")
        await refresh_saved_public_panel()

    @discord.ui.button(label="Set Max Parallel", style=discord.ButtonStyle.primary, row=4)
    async def set_parallel_btn(self, button: discord.ui.Button, interaction: discord.Interaction) -> None:
        if not await ensure_owner_interaction(interaction):
            return
        await interaction.response.send_modal(MaxParallelModal())


async def refresh_saved_public_panel() -> bool:
    channel_id, message_id = get_public_panel_location()
    if not channel_id or not message_id:
        return False
    try:
        channel = bot.get_channel(channel_id)
        if channel is None:
            channel = await bot.fetch_channel(channel_id)
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


@bot.event
async def on_ready() -> None:
    init_db()
    LOGGER.info("Connecté: %s (%s)", bot.user, bot.user.id if bot.user else "?")
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="Vikidia & scripts"))
    restored = await refresh_saved_public_panel()
    if restored:
        LOGGER.info("Panneau public restauré.")


@bot.event
async def on_application_command_error(interaction: discord.Interaction, error: Exception) -> None:
    LOGGER.exception("Erreur commande: %s", error)
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
            f"user={interaction.user} ({interaction.user.id})\n"
            f"command={interaction.command}\n"
            f"error={details}"
        )


@bot.slash_command(name="panel", description="Panneau public pour gérer les scripts")
async def panel(ctx: discord.ApplicationContext) -> None:
    message, created = await upsert_public_panel(ctx.channel)
    audit(ctx.author.id, "panel_upsert", str(message.id), f"created={int(created)}")
    await ctx.respond(
        f"Panneau public {'créé' if created else 'mis à jour'}: {message.jump_url}",
        ephemeral=True,
    )


@bot.slash_command(name="op_panel", description="Panneau OP (owner) en éphémère")
async def op_panel(ctx: discord.ApplicationContext) -> None:
    if not await ensure_owner_ctx(ctx):
        return
    await ctx.respond(embed=build_op_panel_embed(), view=OpPanelView(), ephemeral=True)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


def main() -> None:
    configure_logging()
    init_db()
    token = load_token()
    bot.run(token)


if __name__ == "__main__":
    main()
