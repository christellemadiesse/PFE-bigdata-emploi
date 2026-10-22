"""
Phase 4 - étape 3 : écrire les offres nettoyées et leurs compétences dans PostgreSQL.

Crée (ou remplace) deux tables :
  - offres             : une ligne par offre, prête pour Power BI ;
  - offre_competences  : une ligne par compétence citée dans une offre (liée à offres par id).
Le script peut être relancé : il remplace le contenu à chaque exécution, sans créer de doublons.
"""

import os

from nettoyage import creer_session, extraire_competences, lire_offres
from pyspark.sql import functions as F

spark = creer_session("03_charger")

URL = f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}"    # « postgres » = nom du service dans docker-compose
PROPRIETES = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
    "truncate": "true",    # en cas de relance : on vide la table au lieu de la supprimer
}

offres = lire_offres(spark).cache()
competences = extraire_competences(offres)

# Colonnes retenues pour la base (la description complète reste dans le data lake)
table_offres = offres.select(
    "id", "intitule", "secteur", "secteur_methode", "contrat", "metier_rome", "entreprise", "secteur_employeur",
    "lieu_libelle", "departement", "region", "date_creation", "date_actualisation",
    "experience_ans", "debutant_accepte",
    "salaire_libelle", "salaire_min_annuel", "salaire_max_annuel", "salaire_moyen_annuel",
    F.concat_ws(",", "secteurs_requete").alias("secteurs_requete"),
)

table_offres.write.jdbc(URL, "offres", mode="overwrite", properties=PROPRIETES)
competences.write.jdbc(URL, "offre_competences", mode="overwrite", properties=PROPRIETES)

# Vérification : on relit ce qui vient d'être écrit
print("\n=== Contrôle après écriture ===")
print("Lignes dans offres            :", spark.read.jdbc(URL, "offres", properties=PROPRIETES).count())
print("Lignes dans offre_competences :", spark.read.jdbc(URL, "offre_competences", properties=PROPRIETES).count())

spark.stop()
