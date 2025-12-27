import pywikibot
from datetime import datetime

site = pywikibot.Site('en', 'vikidia')  # adapte si c'est fr ou autre
site.login()  # connecte ton bot

# Fonction pour créer une page annuelle avec contenu et catégorie
def create_annual_page(base_page, header_template, category_name):
    year = datetime.now().year
    page_title = f"{base_page}/{year}"
    page = pywikibot.Page(site, page_title)
    
    if page.exists():
        print(f"Page {page_title} already exists.")
        return
    
    content = f"<noinclude>{{{{{header_template}}}}}\n[[Category:{category_name}|{{{{SUBPAGENAME}}}}]]</noinclude>"
    page.text = content
    page.save(summary=f"Creating annual page {page_title} with header {header_template}")

# Exemple d'utilisation
create_annual_page("Vikidia:Requests/Administrators", "Vikidia:Requests/Administrators/Header", "Requests for administrators")
create_annual_page("Vikidia:The Scholar", "Vikidia:The Scholar/Header", "The Scholar")
create_annual_page("Vikidia:Requests/Bureaucrats", "Vikidia:Requests/Bureaucrats/Header", "Requests for bureaucrats")
create_annual_page("Vikidia:Requests/CheckUsers", "Vikidia:Requests/CheckUsers/Header", "Requests for CheckUsers")
create_annual_page("Vikidia:Requests/Bots", "Vikidia:Requests/Bots/Header", "Requests for bots")

# Fonction pour ajouter une catégorie à toutes les sous-pages
def add_category_to_subpages(parent_page_title, category_name):
    parent_page = pywikibot.Page(site, parent_page_title)
    subpages = parent_page.backlinks(filterRedirects=False)  # récupère les sous-pages et backlinks
    for subpage in subpages:
        if not subpage.exists():
            continue
        if f"[[Category:{category_name}]]" in subpage.text:
            continue
        subpage.text += f"\n[[Category:{category_name}]]"
        subpage.save(summary=f"Adding category {category_name} to subpage {subpage.title()}")

# Exemple pour Administrators
add_category_to_subpages("Vikidia:Requests/Administrators", "Requests for administrators")
