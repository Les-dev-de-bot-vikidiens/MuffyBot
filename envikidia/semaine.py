import os
import pywikibot
from datetime import datetime

# IMPORTANT : dossier Pywikibot (évite les 403 via cron)
os.chdir("/home/ubuntu/pywikibot-scripts/envikidia")

site = pywikibot.Site('en', 'vikidia')
site.login()

# Date actuelle (semaine ISO, lundi = début)
now = datetime.utcnow()
year, week_number, _ = now.isocalendar()

page_title = f"Vikidia:Talk/{year}/{week_number:02d}"
page = pywikibot.Page(site, page_title)

if page.exists():
    print(f"{page_title} already exists.")
    exit()

# Contenu EXACT demandé
page.text = "<noinclude> {{Vikidia:Talk/Head}} </noinclude>"

page.save(
    summary=f"Creating weekly Vikidia:Talk page (week {week_number}, {year})"
)

print(f"{page_title} created successfully.")
