"""
Phase 4 - étape 2 : contrôler le nettoyage, les salaires et l'extraction de compétences.

Ce script N'ÉCRIT RIEN : il affiche des contrôles pour vérifier les règles de nettoyage
(définies dans nettoyage.py) avant de charger les données dans PostgreSQL (03_charger.py).
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from nettoyage import creer_session, extraire_competences, lire_offres

spark = creer_session("02_nettoyer")
offres = lire_offres(spark).cache()

print(f"\nOffres uniques après dédoublonnage : {offres.count()}")

print("\n=== Aperçu ===")
offres.select("id", "intitule", "secteur", "contrat", "experience_ans", "region").show(8, truncate=30)

print("=== Répartition par contrat ===")
offres.groupBy("contrat").count().orderBy(F.desc("count")).show()

print("=== Répartition par secteur ===")
offres.groupBy("secteur").count().orderBy(F.desc("count")).show()

print("=== Offres « débutant accepté » ===")
offres.groupBy("debutant_accepte").count().show()

# ---------------------------------------------------------------------------
# Salaires
# ---------------------------------------------------------------------------
print("=== Salaires : combien d'offres ont un montant annuel exploitable ? ===")
offres.select(
    F.count("*").alias("offres"),
    F.sum(F.col("salaire_libelle").isNotNull().cast("int")).alias("avec_texte_salaire"),
    F.sum(F.col("salaire_moyen_annuel").isNotNull().cast("int")).alias("montant_exploitable"),
).show()

print("=== Salaires : exemples de textes lus et montants annuels obtenus ===")
offres.filter("salaire_libelle is not null").select(
    F.substring("salaire_libelle", 1, 60).alias("texte"), "salaire_min_annuel", "salaire_max_annuel"
).show(12, truncate=False)

print("=== Salaires : textes NON compris (à examiner) ===")
offres.filter("salaire_libelle is not null and salaire_moyen_annuel is null").select(
    F.substring("salaire_libelle", 1, 70).alias("texte")
).groupBy("texte").count().orderBy(F.desc("count")).show(12, truncate=False)

print("=== Salaire moyen annuel par secteur (offres exploitables) ===")
offres.groupBy("secteur").agg(
    F.count("salaire_moyen_annuel").alias("offres"),
    F.round(F.avg("salaire_moyen_annuel"), 0).alias("moyenne"),
    F.round(F.expr("percentile_approx(salaire_moyen_annuel, 0.5)"), 0).alias("mediane"),
).show()

# ---------------------------------------------------------------------------
# Compétences
# ---------------------------------------------------------------------------
competences = extraire_competences(offres).cache()
print(f"\n=== Compétences : {competences.count()} mentions dans {competences.select('id').distinct().count()} offres ===")

print("=== Top 25 des compétences ===")
competences.groupBy("competence", "categorie").count().orderBy(F.desc("count")).show(25, truncate=False)

print("=== Top 5 des compétences par secteur ===")
(
    competences.join(offres.select("id", "secteur"), "id")
    .groupBy("secteur", "competence").count()
    .withColumn("rang", F.row_number().over(Window.partitionBy("secteur").orderBy(F.desc("count"))))
    .filter("rang <= 5")
    .orderBy("secteur", "rang")
    .show(20, truncate=False)
)

print("=== Offres sans aucune compétence détectée ===")
sans = offres.join(competences.select("id").distinct(), "id", "left_anti")
print(f"{sans.count()} offres sur {offres.count()}")

spark.stop()
