# MuffyBot Scripts

Scripts Pywikibot pour Vikidia (fr/en) avec architecture modulaire.

## Architecture

- `muffybot/`: coeur partagé (config, Discord, fichiers, helpers wiki)
- `muffybot/tasks/`: logique métier des bots
- `welcome.py`, `homonym.py`, `categinex.py`, `vandalism.py`, `vandalism_patterns.py`: wrappers de compatibilité
- `daily_bot_logs.py`: publie `bot.logs` (racine) vers le webhook Discord serveur
- `config_backup.py`: backup quotidien de la config runtime (`logs/config_backups/`)
- `envikidia/*.py`: wrappers de compatibilité pour le wiki anglais
- `run_bot.py`: point d'entrée unique optionnel

## Exécution

```bash
python3 run_bot.py welcome
python3 run_bot.py vandalism-fr
python3 run_bot.py envikidia-sandboxreset
python3 run_bot.py vandalism-patterns
python3 run_bot.py daily-report
python3 run_bot.py daily-bot-logs
python3 run_bot.py weekly-report
python3 run_bot.py monthly-report
python3 run_bot.py config-backup
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
- `MUFFYBOT_LOG_FILE` (défaut `bot.logs`, log central au niveau racine)
- `MUFFYBOT_LOG_LEVEL` (défaut `INFO`)
- `MUFFYBOT_LOG_BACKUP_DAYS` (rotation quotidienne, défaut `14`)
- `MUFFYBOT_ENV` (`prod`, `staging`, `test` ; charge `config.<env>.py` / `.env.<env>` si présents)
- `MUFFYBOT_ALLOW_DURING_MAINTENANCE` (`1` pour autoriser une exécution pendant maintenance)
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
- `VANDALISM_REVIEW_RULE_WEIGHT_FACTOR` (poids appliqué aux règles dynamiques en mode review, défaut `0.65`)
- `VANDALISM_BURST_WINDOW_MINUTES` (fenêtre pour détecter les salves d'édition par utilisateur, défaut `12`)
- `VANDALISM_BURST_THRESHOLD` (nb d'éditions mini dans la fenêtre pour activer le signal burst, défaut `3`)
- `VANDALISM_BURST_SCORE_BOOST` (bonus de score par niveau burst, défaut `0.08`)
- `VANDALISM_SENSITIVE_TITLE_BOOST` (bonus de score sur titres sensibles avec signaux risqués, défaut `0.08`)
- `VANDALISM_SENSITIVE_TITLE_KEYWORDS` (mots-clés titres sensibles, CSV)
- `VANDALISM_PATTERN_MIN_TOKEN_HITS` (min occurrences d'un token pour générer une regex auto, défaut `2`)
- `VANDALISM_PATTERN_MIN_PHRASE_HITS` (min occurrences d'une phrase pour regex auto, défaut `3`)
- `VANDALISM_PATTERN_MIN_SUPPORT` (support minimum d'une regex sur corpus vandalisme, défaut `2`)
- `VANDALISM_PATTERN_MIN_PRECISION` (précision minimum retenue pour une regex auto, défaut `0.78`)
- `VANDALISM_PATTERN_MAX_REGEX_RULES` (cap des regex auto chargées, défaut `180`)
- `VANDALISM_PATTERN_ROLLING_WINDOW_DAYS` (fenêtre de précision glissante depuis SQLite, défaut `30`)
- `VANDALISM_PATTERN_MIN_LIVE_HITS` (hits réels mini pour activer pleinement une règle, défaut `6`)
- `VANDALISM_PATTERN_REVIEW_SUPPORT_THRESHOLD` (support mini training pour éviter review, défaut `5`)
- `VANDALISM_PATTERN_EXPIRE_HITS` (hits mini avant expiration possible d'une règle, défaut `20`)
- `VANDALISM_PATTERN_EXPIRE_PRECISION` (précision glissante sous laquelle la règle expire, défaut `0.35`)
- `VANDALISM_FP_WHITELIST_MIN_HITS` (hits mini pour auto-whitelist, défaut `12`)
- `VANDALISM_FP_WHITELIST_PRECISION` (précision seuil auto-whitelist, défaut `0.25`)
- `VANDALISM_PATTERN_VALIDATION_HOLDOUT_RATIO` (pourcentage holdout offline, défaut `20`)
- `HUMAN_REVERT_WINDOW_DAYS` (fenêtre d'analyse des RC humains, défaut `14`)
- `HUMAN_REVERT_MAX_RC_PER_LANG` (nombre max de RC scannés par langue, défaut `4000`)
- `HUMAN_REVERT_MAX_DIFFS_PER_LANG` (nombre max de diffs extraits par langue à chaque run, défaut `250`)
- `HUMAN_REVERT_MAX_CORPUS_ENTRIES` (taille max du corpus humain persistant, défaut `50000`)
- `HUMAN_REVERT_CORPUS_RETENTION_DAYS` (rétention des entrées corpus, défaut `120`)
- `VAULT_ENABLE` (`1` pour activer le chargement secrets Vault)
- `VAULT_SECRETS_FILE` (fichier env injecté par Vault Agent, optionnel)
- `VAULT_ADDR`, `VAULT_TOKEN`, `VAULT_KV_MOUNT`, `VAULT_SECRET_PATH`, `VAULT_TIMEOUT_SECONDS` (accès direct KV v2)

Important:
- Copier `config.example.py` vers `config.py` et compléter les valeurs.
- `config.py` est ignoré par Git (secret local).
- Avec `SERVER_LOG_EVERY_ACTION=1`, le canal serveur recevra un très grand volume de logs.
- Les actions serveur sont aussi historisées localement (`SERVER_ACTIONS_FILE`) avec rotation.
- En cas de panne temporaire Discord, les notifications sont mises en file d'attente dans `logs/discord_queue.json`.
- Les logs Python sont centralisés dans `bot.logs` (racine), avec rotation quotidienne.
- `kill switch`: créer `control/kill.switch` pour bloquer les lancements et arrêter les runs via le panel OP; supprimer le fichier pour réactiver.
- `maintenance mode`: créer `control/maintenance.mode` pour bloquer les runs non autorisés; supprimer le fichier pour sortir de maintenance.
- Le script `vandalism_patterns.py` génère `vandalism_common_patterns.txt`, `vandalism_detection_regex.txt`, `vandalism_pattern_validation.json`, `vandalism_rule_drift_report.txt` et `vandalism_false_positive_whitelist.json`.
- Le script `vandalism.py` alimente aussi `vandalism_intel.sqlite3` (analytics règles et outcomes).

## Cron

Voir `CRONTAB.example`.

## Auto-push securise (systemd)

Un auto-push est disponible via `systemd --user`, avec protections anti-erreur/secrets.

Services `systemd`:

- `muffybot-autopush.timer` (declenchement toutes ~20s)
- `muffybot-autopush.service` (execution d'un cycle)

Installation:

```bash
chmod +x scripts/safe_autopush.sh scripts/install_autopush_systemd.sh scripts/uninstall_autopush_systemd.sh
./scripts/install_autopush_systemd.sh
```

Desinstallation:

```bash
./scripts/uninstall_autopush_systemd.sh
```

Comportement:

- verification toutes les ~20 secondes
- `git add` automatique: non
- commit auto des fichiers suivis et eligibles uniquement (`git commit -m ... -- <paths>`)
- les fichiers non suivis/non ignores sont ignores (ils ne bloquent plus le service)
- les chemins sensibles sont ignores (`config.py`, `user-password.py`, `*.sqlite*`, `discord-bot/venv/*`, etc.)
- si un motif de secret est detecte dans les lignes ajoutees des fichiers eligibles, le cycle est skip
- push auto uniquement si la branche locale est en avance

Logs:

```bash
journalctl --user -u muffybot-autopush.service -f
tail -f ~/.local/state/muffybot-autopush/autopush.log
```

## Environnement de test

Creer un venv de test + fichiers de config de test:

```bash
chmod +x scripts/setup_test_env.sh
./scripts/setup_test_env.sh
source .venv-test/bin/activate
```

Le script cree:

- `.venv-test/`
- `config.test.py` (copie de `config.example.py` si absent)
- `.env.test` (fichier optionnel pour variables de test)

## Branche Git de test (worktree)

Creer une branche de test isolee dans un autre dossier:

```bash
chmod +x scripts/setup_test_worktree.sh
./scripts/setup_test_worktree.sh test/sandbox ../pywikibot-scripts-test
```

Resultat:

- branche locale `test/sandbox`
- worktree separe `../pywikibot-scripts-test`

## Dashboard leger

Dashboard HTTP sans dependance externe (status Git, timer auto-push, rapports de taches, logs auto-push):

```bash
python3 dashboard.py
```

Puis ouvrir:

- `http://127.0.0.1:8765`

Variables optionnelles:

- `MUFFY_DASHBOARD_HOST` (defaut `127.0.0.1`)
- `MUFFY_DASHBOARD_PORT` (defaut `8765`)

## Healthcheck Pywikibot (Uptime Kuma)

Le bot anti-vandalisme Pywikibot expose un endpoint local dedie:

- URL: `http://127.0.0.1:8798/healthz`
- variables: `PYWIKIBOT_HEALTH_ENABLE` (`1/0`), `PYWIKIBOT_HEALTH_HOST` (defaut `127.0.0.1`), `PYWIKIBOT_HEALTH_PORT` (defaut `8798`)
