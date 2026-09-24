"""
Script d'ingestion : collecte des offres d'emploi via l'API France Travail.

Étape 1 (Authentification) -> Étape 2 (Requête) -> Étape 3 (Sauvegarde brute)

Avant d'exécuter ce script :
1. Créer un compte sur https://francetravail.io/inscription
2. Créer une application pour obtenir un CLIENT_ID et un CLIENT_SECRET
3. S'abonner à l'API "Offres d'emploi v2"
4. Placer CLIENT_ID et CLIENT_SECRET dans un fichier .env (jamais dans le code en clair)
"""

import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# Charge les identifiants depuis le fichier .env (non versionné sur Git)
load_dotenv()
CLIENT_ID = os.getenv("FRANCE_TRAVAIL_CLIENT_ID")
CLIENT_SECRET = os.getenv("FRANCE_TRAVAIL_CLIENT_SECRET")

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"


def get_access_token():
    """
    ÉTAPE 1 — Authentification.
    L'API France Travail utilise le protocole OAuth2 (grant_type=client_credentials).
    On envoie CLIENT_ID + CLIENT_SECRET, et l'API renvoie un jeton temporaire
    (valable environ 30 minutes) qu'il faut ensuite utiliser pour chaque requête.
    """
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "api_offresdemploiv2 o2dsoffre",
    }
    response = requests.post(TOKEN_URL, data=payload)

    # DEBUG temporaire : affiche le détail renvoyé par l'API en cas d'erreur
    if response.status_code != 200:
        print("Code retourné :", response.status_code)
        print("Message de l'API :", response.text)
        print("CLIENT_ID (repr) :", repr(CLIENT_ID))
        print("CLIENT_ID longueur :", len(CLIENT_ID) if CLIENT_ID else 0)
        print("CLIENT_SECRET (repr) :", repr(CLIENT_SECRET))
        print("CLIENT_SECRET longueur :", len(CLIENT_SECRET) if CLIENT_SECRET else 0)

    response.raise_for_status()  # lève une erreur si l'authentification échoue
    return response.json()["access_token"]


def search_offers(token, secteur_mots_cles, region=None, page=0):
    """
    ÉTAPE 2 — Requête.
    On interroge l'endpoint de recherche avec le jeton obtenu, en filtrant
    par mots-clés (ex. "développeur python", "data analyst") et éventuellement
    par région. L'API renvoie les offres au format JSON, avec pagination.
    """
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "motsCles": secteur_mots_cles,
        "range": f"{page * 50}-{page * 50 + 49}",  # 50 offres par page
    }
    if region:
        params["region"] = region

    response = requests.get(SEARCH_URL, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def save_raw(data, secteur):
    """
    ÉTAPE 3 — Sauvegarde brute.
    On enregistre la réponse telle quelle (sans transformation) dans le
    Data Lake local, avec un nom de fichier horodaté. C'est ce fichier
    brut qui sera lu et nettoyé plus tard par le job PySpark.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dossier = "data/raw"
    os.makedirs(dossier, exist_ok=True)
    chemin = f"{dossier}/{secteur}_{timestamp}.json"

    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"{len(data.get('resultats', []))} offres sauvegardées dans {chemin}")


if __name__ == "__main__":
    secteurs = ["developpeur data", "power bi", "analyste financier"]

    token = get_access_token()

    for secteur in secteurs:
        resultats = search_offers(token, secteur_mots_cles=secteur)
        save_raw(resultats, secteur.replace(" ", "_"))
