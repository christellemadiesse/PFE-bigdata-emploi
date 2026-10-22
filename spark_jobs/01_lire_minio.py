"""
Phase 4 - étape 1 : vérifier que Spark sait lire les offres brutes dans MinIO.

Ce script ne transforme rien. Il lit tous les JSON du bucket, aplatit la liste
d'offres, et affiche quelques chiffres pour contrôle.

Lancement (depuis PowerShell, dans le dossier du projet) : voir les commandes données avec ce fichier.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Session Spark configurée pour parler à MinIO via le protocole S3 (connecteur "s3a")
spark = (
    SparkSession.builder.appName("01_lire_minio")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")       # "minio" = nom du service dans docker-compose
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")           # MinIO exige des adresses de type /bucket/fichier
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")     # pas de HTTPS en local
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. Lecture de tous les fichiers JSON de tous les dossiers datés.
#    multiLine=true : nos fichiers sont écrits sur plusieurs lignes (indentés).
brut = spark.read.option("multiLine", "true").json("s3a://offres-emploi-raw/*/*.json")
print("\n=== Colonnes lues dans les fichiers ===")
brut.printSchema()

# 3. Chaque fichier contient une liste "resultats" : explode() crée une ligne par offre.
offres = brut.select(F.explode("resultats").alias("offre")).select("offre.*")

total = offres.count()
uniques = offres.select("id").distinct().count()
print(f"\nOffres lues : {total} | offres uniques : {uniques}")

# 4. Aperçu de quelques colonnes
offres.select("id", "intitule", "typeContrat", "dateCreation").show(5, truncate=40)

spark.stop()
