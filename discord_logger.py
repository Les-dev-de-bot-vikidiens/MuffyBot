#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compatibility layer for existing scripts importing discord_logger."""

from muffybot.discord import flush_logs, log_to_discord, send_discord_webhook

__all__ = ["log_to_discord", "send_discord_webhook", "flush_logs"]
