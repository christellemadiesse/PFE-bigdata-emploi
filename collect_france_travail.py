"""
Script d'ingestion v2 : collecte des offres d'emploi via l'API France Travail.

Nouveautés par rapport à la v1 :
- pagination : on récupère plusieurs pages de 50 offres par mot-clé ;
- mots-clés équilibrés : 6 par secteur (IT, Data, Finance) ;
- chaque fichier JSON porte le secteur et le mot-clé de la requête.

Prérequis : fichier .env avec FRANCE_TRAVAIL_CLIENT_ID et FRANCE_TRAVAIL_CLIENT_SECRET.
"""

import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
CLIENT_ID = os.getenv("FRANCE_TRAVAIL_CLIENT_ID")
CLIENT_SECRET = os.getenv("FRANCE_TRAVAIL_CLIENT_SECRET")

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

# Même nombre de mots-clés par secteur, pour ne pas fausser la comparaison entre secteurs
MOTS_CLES = {
    "IT": [
        "developpeur python", "developpeur java", "devops",
        "administrateur systemes reseaux", "ingenieur cloud", "cybersecurite",
    ],
    "Data": [
        "data engineer", "data analyst", "data scientist",
        "power bi", "ingenieur big data", "business intelligence",
    ],
    "Finance": [
        "analyste financier", "controleur de gestion", "comptable",
        "auditeur financier", "gestionnaire de portefeuille", "tresorier",
    ],
}

TAILLE_PAGE = 50      # nombre d'offres demandées par requête
PAGES_MAX = 3         # 3 pages x 50 = 150 offres maximum par mot-clé
PAUSE = 0.2           # secondes entre deux requêtes (limite de l'API : 10 requêtes/seconde)


def get_access_token():
    """Étape 1 : obtenir un jeton OAuth2 (valable environ 25 minutes)."""
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "api_offresdemploiv2 o2dsoffre",
    }
    response = requests.post(TOKEN_URL, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]


def search_offers(token, mot_cle, page=0):
    """Étape 2 : demander une page d'offres pour un mot-clé. Renvoie la liste des offres."""
    headers = {"Authorization": f"Bearer {token}"}
    debut = page * TAILLE_PAGE
    params = {
        "motsCles": mot_cle,
        "range": f"{debut}-{debut + TAILLE_PAGE - 1}",
    }
    response = requests.get(SEARCH_URL, headers=headers, params=params)
    if response.status_code == 204:      # 204 = aucune offre pour cette recherche
        return []
    response.raise_for_status()          # 200 et 206 (réponse partielle) sont normaux
    return response.json().get("resultats", [])


def collecter_mot_cle(token, mot_cle):
    """Parcourt les pages une à une jusqu'à PAGES_MAX, ou jusqu'à la dernière page."""
    offres = []
    for page in range(PAGES_MAX):
        resultats = search_offers(token, mot_cle, page)
        offres.extend(resultats)
        time.sleep(PAUSE)
        if len(resultats) < TAILLE_PAGE:  # page incomplète = il n'y en a plus après
            break
    return offres


def save_raw(offres, secteur, mot_cle):
    """Étape 3 : sauvegarde brute, sans transformation, avec le secteur et le mot-clé."""
    horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
    dossier = "data/raw"
    os.makedirs(dossier, exist_ok=True)
    nom = mot_cle.replace(" ", "_")
    chemin = f"{dossier}/{secteur}_{nom}_{horodatage}.json"

    contenu = {"secteur": secteur, "motCle": mot_cle, "resultats": offres}
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(contenu, f, ensure_ascii=False, indent=2)
    print(f"  {len(offres):3d} offres -> {chemin}")


if __name__ == "__main__":
    token = get_access_token()
    total = 0

    for secteur, mots in MOTS_CLES.items():
        print(f"\n=== Secteur {secteur} ===")
        for mot_cle in mots:
            offres = collecter_mot_cle(token, mot_cle)
            save_raw(offres, secteur, mot_cle)
            total += len(offres)

    print(f"\nTerminé : {total} offres collectées (doublons non retirés).")
