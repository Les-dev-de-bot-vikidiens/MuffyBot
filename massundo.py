import pywikibot
from datetime import datetime

# ================= CONFIG =================
USERNAME = "NomDutilisateur"  # IP ou nom d'utilisateur
SUMMARY = "Annulation massive de modifications non constructives"
DRY_RUN = False  # ⚠️ Mettre False pour réellement annuler
LOG_FILE = "mass_undo_log.txt"
# ==========================================

def log(message):
    timestamp = datetime.utcnow().strftime("[%Y-%m-%d %H:%M:%S UTC] ")
    line = timestamp + message
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def main():
    site = pywikibot.Site("fr", "vikidia")
    site.login()

    log(f"Début undo massif pour : {USERNAME}")
    log(f"DRY_RUN = {DRY_RUN}")

    undone_revs = set()

    for contrib in site.usercontribs(user=USERNAME, total=None):
        page = pywikibot.Page(site, contrib["title"])
        rev_id = contrib["revid"]

        if rev_id in undone_revs:
            continue

        if page.isRedirectPage():
            continue

        try:
            revisions = list(page.revisions())
            rev_ids = [r.revid for r in revisions]

            if rev_id not in rev_ids:
                continue

            index = rev_ids.index(rev_id)
            if index + 1 >= len(revisions):
                log(f"{page.title()} → impossible d'annuler (pas de version précédente)")
                continue

            old_rev = revisions[index + 1]

            log(f"Undo {page.title()} (rev {rev_id} → {old_rev.revid})")

            if not DRY_RUN:
                page.text = old_rev.text
                page.save(
                    summary=SUMMARY,
                    minor=False
                )

            undone_revs.add(rev_id)

        except Exception as e:
            log(f"ERREUR sur {page.title()} : {e}")
            continue

    log("Undo massif terminé.")

if __name__ == "__main__":
    main()
