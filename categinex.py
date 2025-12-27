import pywikibot
import requests
import threading
import time
import os
from datetime import datetime
STATUS_URL = "https://bothulkvikidia.pythonanywhere.com/status"
FICHIER_PAGES_TRAITEES = "pages_traitees.txt"

os.chdir("/home/ubuntu/pywikibot-scripts")

def envoyer_ping():
    while True:
        try:
            requests.get(STATUS_URL)
        except Exception as e:
            print(f"Erreur en envoyant le statut : {e}")
        time.sleep(60)  # Ping toutes les 60 secondes

# Lancer le thread en arrière-plan
threading.Thread(target=envoyer_ping, daemon=True).start()

# Connexion au site Vikidia
site = pywikibot.Site("fr", "vikidia")
site.login()

def charger_pages_traitees():
    """Charge la liste des pages déjà traitées à partir du fichier."""
    if os.path.exists(FICHIER_PAGES_TRAITEES):
        with open(FICHIER_PAGES_TRAITEES, "r", encoding="utf-8") as f:
            return set(f.read().splitlines())
    return set()

LOG_PAGE = "User:MuffyBot/Logs"

def log_action(message: str):
    """Ajoute un message dans la page de logs avec la date et l'heure."""
    page = pywikibot.Page(site, LOG_PAGE)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    log_entry = f"* {now} — {message}\n"

    try:
        # Préserve le contenu existant
        content = page.text if page.exists() else ""
        page.text = log_entry + content  # on met le plus récent en haut
        page.save(summary="Updating bot logs")
    except Exception as e:
        print(f"Erreur lors de l’écriture du log: {e}")

def charger_pages_a_ignorer():
    ignore_page = pywikibot.Page(site, "Utilisateur:MuffyBot/Ignore")
    if ignore_page.exists():
        return set([line.strip() for line in ignore_page.text.splitlines() if line.strip()])
    return set()

def enregistrer_page_traitee(page_title):
    """Ajoute une page traitée au fichier."""
    with open(FICHIER_PAGES_TRAITEES, "a", encoding="utf-8") as f:
        f.write(page_title + "\n")

def supprimer_categories_inexistantes(page):
    """Supprime les catégories inexistantes d'une page."""
    if not page.exists():
        print(f"❌ La page '{page.title()}' n'existe pas.")
        return

    texte = page.text
    categories = page.categories()
    texte_modifie = texte
    categories_supprimees = []

    for cat in categories:
        if not cat.exists():
            cat_syntaxe = f"[[{cat.title()}]]"
            texte_modifie = texte_modifie.replace(cat_syntaxe, "")
            categories_supprimees.append(cat.title())

    if texte != texte_modifie:
        page.text = texte_modifie
        page.save(f"Suppression de catégories inexistantes : {', '.join(categories_supprimees)}")
        print(f"✅ Catégories supprimées sur '{page.title()}': {categories_supprimees}")
        log_action("Catégories supprimées sur '{page.title()}': {categories_supprimees}")
        enregistrer_page_traitee(page.title())
    else:
        print(f"⚠️ Aucune catégorie inexistante sur '{page.title()}'.")

def traiter_toutes_les_pages():
    """Parcourt toutes les pages de l'espace principal et supprime les catégories inexistantes si la page n'a pas encore été traitée."""
    pages_traitees = charger_pages_traitees()

    for page in site.allpages(namespace=0):
        if page.title() not in pages_traitees:
            supprimer_categories_inexistantes(page)
        else:
            print(f"🔄 Page '{page.title()}' déjà traitée, passage à la suivante.")
def traiter_toutes_les_pages():
    """Parcourt toutes les pages de l'espace principal et supprime les catégories inexistantes si la page n'a pas encore été traitée et n'est pas ignorée."""
    pages_traitees = charger_pages_traitees()
    pages_a_ignorer = charger_pages_a_ignorer()  # <-- nouveau

    for page in site.allpages(namespace=0):
        if page.title() in pages_a_ignorer:
            print(f"🚫 Page '{page.title()}' est dans la liste d'ignore, passage à la suivante.")
            continue
        if page.title() not in pages_traitees:
            supprimer_categories_inexistantes(page)
        else:
            print(f"🔄 Page '{page.title()}' déjà traitée, passage à la suivante.")
# Lancer le script
traiter_toutes_les_pages()
