# 🤖 MuffyBot

<div align="center">

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pywikibot](https://img.shields.io/badge/Pywikibot-Latest-green?style=for-the-badge&logo=wikipedia&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)

**Scripts Pywikibot pour Vikidia & enVikidia**

*Automatisation de tâches de maintenance et d'organisation sur wikis MediaWiki*

[Installation](#️-installation) • [Utilisation](#️-utilisation) • [Scripts](#-scripts-disponibles) • [Licence](#-licence)

</div>

---

## 📋 À propos

**MuffyBot** est un ensemble de scripts Pywikibot destinés à automatiser des tâches de maintenance et d'organisation sur des wikis MediaWiki, principalement **Vikidia** (fr) et **enVikidia** (enVD).

> 📌 **Note importante** : Ce dépôt contient uniquement le code source. Les fichiers sensibles, les logs et les caches sont volontairement exclus du versionnage Git.

---

## ✨ Scripts disponibles

### 📘 Vikidia (fr)

| Script | Description |
|--------|-------------|
| 🔁 **Mass undo** | Annulation en masse de modifications non constructives ou problématiques |
| 🗂️ **Suppression des catégories inexistantes** | Nettoyage automatique des catégories rouges |
| 🧭 **Suppression des portails sur pages d'homonymie** | Retrait des modèles de portail sur les pages d'homonymie |
| 👋 **Bienvenue des nouveaux utilisateurs** | Message automatique sur la page de discussion des nouveaux comptes |

### 🌍 enVikidia (enVD)

| Script | Description |
|--------|-------------|
| 🧪 **Réinitialisation du Sandbox** | Remise à zéro régulière du bac à sable |
| 📅 **Création automatique des pages temporelles** | Génération automatique des pages de la semaine, pages annuelles et autres pages périodiques |

---

## ⚙️ Installation

### 📋 Prérequis

- 🐍 **Python 3.9** ou supérieur
- 🤖 **Pywikibot** (dernière version)
- 👤 Un compte wiki disposant des droits nécessaires pour les actions effectuées

### 🔐 Configuration

Pour des raisons de sécurité, les fichiers suivants **ne sont pas versionnés** :

```
user-config.py
user-password.py
```

Ces fichiers doivent être créés localement conformément à la [documentation officielle de Pywikibot](https://www.mediawiki.org/wiki/Manual:Pywikibot).

> 🔒 **Sécurité** : Aucun identifiant ni mot de passe n'est stocké dans ce dépôt.

### 📦 Installation des dépendances

```bash
# Cloner le dépôt
git clone https://github.com/votre-username/muffybot.git](https://github.com/Les-dev-de-bot-vikidiens/MuffyBot.git
cd muffybot

# Installer Pywikibot
pip install pywikibot
```

---

## ▶️ Utilisation

Chaque script est conçu pour être lancé manuellement depuis l'environnement Pywikibot.

```bash
python pwb.py nom_du_script.py
```

### 🚨 Avertissement important

> ⚠️ **Attention** : Les scripts sont fournis tels quels (« as-is »).
>
> - ❌ Aucune garantie n'est donnée quant à leur fonctionnement ou compatibilité
> - 👤 L'utilisateur est entièrement responsable de l'usage qu'il en fait
> - ✅ Les autorisations nécessaires sur Vikidia / enVikidia doivent **impérativement** être obtenues avant toute utilisation
>
> 🚫 **L'exécution de scripts sans droits appropriés peut enfreindre les règles du wiki concerné.**

---

## 🧑‍💻 Auteur

<table>
  <tr>
    <td align="center">
      <a href="https://github.com/luffythebest-37">
        <img src="https://github.com/luffythebest-37.png" width="100px;" alt="Muffy"/>
        <br />
        <sub><b>Muffy</b></sub>
      </a>
      <br />
      <sub>🛠️ Développeur de bots Pywikibot & Discord</sub>
      <br />
      <sub>📘 Contributeur Wikis (Vikidia)</sub>
      <br />
      <sub>💡 Automatisation, maintenance et outils communautaires</sub>
    </td>
  </tr>
</table>

---

## Développeurs 

<!-- ALL-DEVS-LIST:START -->
<table>
  <tr>
    <td align="center">
      <a href="https://github.com/CelianVD">
        <img src="https://github.com/CelianVD.png" width="100px;" alt="Célian"/>
        <br />
        <sub><b>Célian >Cb></sub>
      </a>
      <br />
      <sub>🛠️ Développeur de BotCelian pour Vikidia</sub>
      <br />
      <sub>📘 Contributeur Wikis (Vikidia)</sub>
      <br />
    </td>
  </tr>
</table>

<table>
  <tr>
    <td align="center">
      <a href="https://github.com/janusdevikidia-37">
        <img src="https://github.com/janusdevikidia.png" width="100px;" alt="Janus"/>
        <br />
        <sub><b>Janus</b></sub>
      </a>
      <br />
      <sub>🛠️ Développeur de bots Pywikibot</sub>
      <br />
      <sub>📘 Contributeur Wikis (Vikidia)</sub>
      <br />
      <sub> Développeur & Designer Flask </sub>
    </td>
  </tr>
</table>

## 🤝 Contributeurs

Les contributions sont les bienvenues ! N'hésitez pas à ouvrir une issue ou une pull request.

<!-- ALL-CONTRIBUTORS-LIST:START -->
<!-- Ajoutez ici les contributeurs futurs -->
<!-- ALL-CONTRIBUTORS-LIST:END -->

---

## 📜 Licence

Ce projet est sous licence **MIT**. 

Si vous utilisez ou modifiez ce projet, merci de mentionner les contributeurs originaux.

```
MIT License - Copyright (c) 2025 Muffy
```

---

<div align="center">

**⭐ Si ce projet vous est utile, n'hésitez pas à lui donner une étoile !**

Made with ❤️ by [Muffy](https://github.com/luffythebest-37)

</div>
