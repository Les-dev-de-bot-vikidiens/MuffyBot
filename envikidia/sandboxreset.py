import pywikibot
from datetime import datetime, timezone, timedelta

site = pywikibot.Site('en', 'vikidia')  # adapte si besoin
site.login()

PAGE_TITLE = "Vikidia:Sandbox"
RESET_CONTENT = "<!-- PLEASE DO NOT MODIFY THIS LINE -->{{/Header}}<!-- PLEASE DO NOT MODIFY THIS LINE -->"
DELAY_MINUTES = 3

page = pywikibot.Page(site, PAGE_TITLE)

if not page.exists():
    print("Sandbox does not exist.")
    exit()

# Récupération de la dernière révision
last_revision = next(page.revisions(total=1))
last_edit_time = last_revision.timestamp.replace(tzinfo=timezone.utc)
now = datetime.now(timezone.utc)

time_diff = now - last_edit_time

# Vérification du délai
if time_diff < timedelta(minutes=DELAY_MINUTES):
    print("Last edit is too recent, no reset needed.")
    exit()

# Vérification si le contenu est déjà celui par défaut
if page.text.strip() == RESET_CONTENT:
    print("Sandbox already reset.")
    exit()

# Reset de la sandbox
page.text = RESET_CONTENT
page.save(
    summary="Reset sandbox to default content (automatic cleanup)",
    minor=False
)

print("Sandbox successfully reset.")
