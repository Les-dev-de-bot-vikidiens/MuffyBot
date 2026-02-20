#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import time

from muffybot.discord import log_server_action, log_server_diagnostic, send_task_report
from muffybot.env import load_dotenv
from muffybot.logging_setup import configure_root_logging
from muffybot.tasks import categinex, daily_report, doctor, homonym, monthly_report, weekly_report, welcome
from muffybot.tasks import config_backup, daily_bot_logs
from muffybot.tasks import envikidia_annual_pages, envikidia_sandboxreset, envikidia_weekly_talk
from muffybot.tasks.vandalism_patterns import main as vandalism_patterns_main
from muffybot.tasks.vandalism import main_en as vandalism_en_main
from muffybot.tasks.vandalism import main_fr as vandalism_fr_main

TASKS = {
    "welcome": welcome.main,
    "homonym": homonym.main,
    "categinex": categinex.main,
    "vandalism-fr": vandalism_fr_main,
    "vandalism-en": vandalism_en_main,
    "vandalism-patterns": vandalism_patterns_main,
    "daily-report": daily_report.main,
    "daily-bot-logs": daily_bot_logs.main,
    "weekly-report": weekly_report.main,
    "monthly-report": monthly_report.main,
    "config-backup": config_backup.main,
    "doctor": doctor.main,
    "envikidia-annual": envikidia_annual_pages.main,
    "envikidia-sandboxreset": envikidia_sandboxreset.main,
    "envikidia-weekly-talk": envikidia_weekly_talk.main,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a MuffyBot task")
    parser.add_argument("task", choices=sorted(TASKS.keys()), help="Task name to run")
    args = parser.parse_args()
    task_name = args.task
    started = time.monotonic()
    load_dotenv()
    configure_root_logging(logger_name="run_bot.py")
    try:
        exit_code = int(TASKS[task_name]())
    except Exception as exc:
        load_dotenv()
        duration = time.monotonic() - started
        log_server_action(
            "task_runner_exception",
            script_name="run_bot.py",
            level="CRITICAL",
            include_runtime=True,
            context={"task": task_name, "error": str(exc)[:300]},
        )
        log_server_diagnostic(
            message=f"Echec run_bot sur la tâche {task_name}",
            level="CRITICAL",
            script_name="run_bot.py",
            context={"task": task_name},
            exception=exc,
        )
        send_task_report(
            script_name="run_bot.py",
            status="FAILED",
            duration_seconds=duration,
            details=f"Tâche {task_name} crashée: {exc}",
            stats={"task": task_name},
            level="CRITICAL",
            channel="server",
        )
        return 1

    if exit_code != 0:
        load_dotenv()
        duration = time.monotonic() - started
        log_server_action(
            "task_runner_non_zero_exit",
            script_name="run_bot.py",
            level="WARNING",
            context={"task": task_name, "exit_code": exit_code, "duration_seconds": round(duration, 2)},
        )
        send_task_report(
            script_name="run_bot.py",
            status="WARNING",
            duration_seconds=duration,
            details=f"Tâche {task_name} terminée avec code {exit_code}",
            stats={"task": task_name, "exit_code": exit_code},
            level="WARNING",
            channel="server",
        )
    else:
        load_dotenv()
        log_server_action(
            "task_runner_success",
            script_name="run_bot.py",
            level="SUCCESS",
            context={"task": task_name, "duration_seconds": round(time.monotonic() - started, 2)},
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
