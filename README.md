# MuffyBot Scripts

Scripts Pywikibot pour Vikidia (fr/en) avec architecture modulaire.

## Architecture

- `muffybot/`: coeur partagé (config, Discord, fichiers, helpers wiki)
- `muffybot/tasks/`: logique métier des bots
- `welcome.py`, `homonym.py`, `categinex.py`, `vandalism.py`: wrappers de compatibilité
- `envikidia/*.py`: wrappers de compatibilité pour le wiki anglais
- `run_bot.py`: point d'entrée unique optionnel

## Exécution

```bash
python3 run_bot.py welcome
python3 run_bot.py vandalism-fr
python3 run_bot.py envikidia-sandboxreset
python3 run_bot.py daily-report
python3 run_bot.py weekly-report
python3 run_bot.py monthly-report
python3 run_bot.py doctor
```

## Configuration (`config.py`)

- `DISCORD_WEBHOOK_MAIN` (URL webhook Discord complete)
- `DISCORD_WEBHOOK_ERRORS` (URL webhook Discord complete)
- `DISCORD_WEBHOOK_VANDALISM` (URL webhook Discord complete)
- `DISCORD_WEBHOOK_SERVER_LOGS` (canal serveur ultra détaillé)
- `DISCORD_MENTION_ON_CRITICAL` (`1`/`0`)
- `DISCORD_CRITICAL_LEVELS` (ex: `CRITICAL,FAILED`)
- `DISCORD_CRITICAL_USER_ID` (id Discord à mentionner)
- `DISCORD_CRITICAL_USERNAME` (fallback mention texte)
- `SERVER_LOG_EVERY_ACTION` (`1` pour log serveur de chaque action métier)
- `SERVER_ACTION_LOG_TO_DISCORD` (`1` pour pousser chaque action vers Discord, `0` pour fichier local uniquement)
- `SERVER_LOG_INCLUDE_SECRETS` (`1` pour inclure les valeurs sensibles en logs serveur)
- `SERVER_ACTIONS_FILE` (optionnel, défaut `logs/server_actions.jsonl`)
- `SERVER_ACTION_LOG_MAX_MB` (rotation du fichier d'actions serveur)
- `SERVER_ACTION_LOG_BACKUPS` (nombre de backups de rotation)
- `MISTRAL_API_KEY` (recommandé pour anti-vandalisme IA)
- `STATUS_URL` (optionnel, pour `categinex`)
- `ENABLE_STATUS_PING` (`1` pour activer, sinon désactivé)
- `DAILY_REPORT_WINDOW_HOURS` (optionnel, défaut `24`)
- `WEEKLY_REPORT_WINDOW_HOURS` (optionnel, défaut `168`)
- `MONTHLY_REPORT_WINDOW_DAYS` (optionnel, défaut `30`)
- `REPORT_CRITICAL_ERROR_THRESHOLD` (seuil passage en `CRITICAL`)
- `REPORT_CRITICAL_QUEUE_THRESHOLD` (seuil queue Discord critique)
- `REPORT_CRITICAL_REVERT_THRESHOLD` (seuil volume reverts critique)
- `TASK_REPORTS_FILE` (optionnel, défaut `logs/task_reports.jsonl`)
- `DOCTOR_SEND_TEST_MESSAGES` (`1` pour envoyer des pings de test Discord via `doctor`)
- `DOCTOR_QUEUE_WARNING_THRESHOLD` (seuil d'alerte sur la queue Discord)
- `HOMONYM_MAX_PAGES_PER_RUN` (limite de pages homonymie traitées par exécution, défaut `1500`)

Important:
- Copier `config.example.py` vers `config.py` et compléter les valeurs.
- `config.py` est ignoré par Git (secret local).
- Avec `SERVER_LOG_EVERY_ACTION=1`, le canal serveur recevra un très grand volume de logs.
- Les actions serveur sont aussi historisées localement (`SERVER_ACTIONS_FILE`) avec rotation.
- En cas de panne temporaire Discord, les notifications sont mises en file d'attente dans `logs/discord_queue.json`.

## Cron

Voir `CRONTAB.example`.
