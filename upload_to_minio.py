"""
Envoie les fichiers JSON bruts de data/raw vers le Data Lake MinIO.

- Lit les identifiants MinIO dans le fichier .env (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD).
- Range chaque fichier dans le bucket "offres-emploi-raw", sous un dossier daté
  (ex. 2026-10-21/IT_devops_20261021_110907.json).
- Ne renvoie pas un fichier déjà présent : le script peut être relancé sans risque.

Prérequis : pip install minio
"""

import os
import re
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

ENDPOINT = "localhost:9000"            # port de l'API MinIO (pas la console 9001)
BUCKET = "offres-emploi-raw"
DOSSIER_LOCAL = Path("data/raw")

client = Minio(
    ENDPOINT,
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False,                      # pas de HTTPS en local
)


def date_du_fichier(nom):
    """Extrait la date AAAAMMJJ du nom de fichier (…_20261021_110907.json) -> 2026-10-21."""
    m = re.search(r"_(\d{4})(\d{2})(\d{2})_\d{6}\.json$", nom)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else "sans-date"


def existe(objet):
    try:
        client.stat_object(BUCKET, objet)
        return True
    except S3Error:
        return False


if __name__ == "__main__":
    if not client.bucket_exists(BUCKET):
        raise SystemExit(f"Le bucket {BUCKET} n'existe pas : crée-le dans la console MinIO.")

    envoyes = ignores = 0
    for chemin in sorted(DOSSIER_LOCAL.glob("*.json")):
        objet = f"{date_du_fichier(chemin.name)}/{chemin.name}"
        if existe(objet):
            ignores += 1
            continue
        client.fput_object(BUCKET, objet, str(chemin), content_type="application/json")
        envoyes += 1
        print(f"envoyé : {objet}")

    print(f"\nTerminé : {envoyes} fichiers envoyés, {ignores} déjà présents.")
