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
```

## Variables d'environnement (.env)

- `DISCORD_WEBHOOK_MAIN` (URL webhook Discord complete)
- `DISCORD_WEBHOOK_ERRORS` (URL webhook Discord complete)
- `DISCORD_WEBHOOK_VANDALISM` (URL webhook Discord complete)
- `MISTRAL_API_KEY` (recommandé pour anti-vandalisme IA)
- `STATUS_URL` (optionnel, pour `categinex`)
- `ENABLE_STATUS_PING` (`1` pour activer, sinon désactivé)

Important:
- Les valeurs exemple comme `https://discord.com/api/webhooks/VOTRE_WEBHOOK_ICI` sont ignorées volontairement.
- Tant que ces placeholders ne sont pas remplacés par de vraies URLs Discord, aucun message ne sera envoyé.
- En cas de panne temporaire Discord, les notifications sont mises en file d'attente dans `logs/discord_queue.json`.

## Cron

Voir `CRONTAB.example`.
