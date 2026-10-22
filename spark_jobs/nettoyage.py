"""
Fonctions communes de la Phase 4 : lecture des offres brutes, nettoyage, normalisation, compétences.

Ce module est importé par 02_nettoyer.py (contrôles) et 03_charger.py (écriture dans PostgreSQL),
afin que les deux scripts utilisent exactement les mêmes règles.
"""

import os
import re
import unicodedata

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from referentiel_competences import COMPETENCES

CHEMIN_BRUT = "s3a://offres-emploi-raw/*/*.json"

# Lettres accentuées -> lettres simples (pour comparer des textes sans se soucier des accents)
ACCENTS_DE = "éèêëîïôûùàâç"
ACCENTS_VERS = "eeeeiiouuaac"

REGIONS = {
    "Auvergne-Rhône-Alpes": ["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"],
    "Bourgogne-Franche-Comté": ["21", "25", "39", "58", "70", "71", "89", "90"],
    "Bretagne": ["22", "29", "35", "56"],
    "Centre-Val de Loire": ["18", "28", "36", "37", "41", "45"],
    "Corse": ["2A", "2B"],
    "Grand Est": ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"],
    "Hauts-de-France": ["02", "59", "60", "62", "80"],
    "Île-de-France": ["75", "77", "78", "91", "92", "93", "94", "95"],
    "Normandie": ["14", "27", "50", "61", "76"],
    "Nouvelle-Aquitaine": ["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"],
    "Occitanie": ["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"],
    "Pays de la Loire": ["44", "49", "53", "72", "85"],
    "Provence-Alpes-Côte d'Azur": ["04", "05", "06", "13", "83", "84"],
    "Outre-mer": ["971", "972", "973", "974", "975", "976", "977", "978", "986", "987", "988"],
}

SALAIRE_MIN_VALIDE = 12000     # en dessous, la valeur est jugée aberrante (erreur de saisie ou unité non reconnue)
SALAIRE_MAX_VALIDE = 300000
HEURES_PAR_AN = 1820           # 35 h x 52 semaines, pour convertir un taux horaire en salaire annuel


def sans_accents(texte):
    """Minuscules et sans accents : « Île-de-France » -> « ile-de-france »."""
    return "".join(c for c in unicodedata.normalize("NFD", texte.lower()) if unicodedata.category(c) != "Mn")


def creer_session(nom):
    """Session Spark configurée pour lire MinIO (protocole S3 « s3a »)."""
    spark = (
        SparkSession.builder.appName(nom)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")   # « minio » = nom du service dans docker-compose
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")                   # 8 morceaux suffisent pour ce volume
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _ajouter_salaires(offres):
    """Transforme le texte du salaire (« Annuel de 40000.0 Euros à 50000.0 Euros sur 12.0 mois ») en montants annuels."""
    libelle = F.col("salaire_libelle")
    periode = F.lower(F.regexp_extract(libelle, r"(?i)^\s*(annuel|mensuel|horaire|hebdomadaire)", 1))
    # On s'arrête avant « sur 12 mois » ou avant un commentaire après un tiret (« - 13ème mois »), qui contiennent des chiffres
    avant_sur = F.split(libelle, r"\s+(?:sur|[-–])\s+")[0]
    avant_sur = F.regexp_replace(avant_sur, r"(\d)\s+(\d{3})(?!\d)", "$1$2")   # « 2 500 » -> « 2500 »
    nombres = F.regexp_extract_all(avant_sur, F.lit(r"(\d+(?:[.,]\d+)?)"), F.lit(1))
    plus_petit = F.regexp_replace(F.element_at(nombres, 1), ",", ".").cast("double")
    plus_grand = F.regexp_replace(F.element_at(nombres, -1), ",", ".").cast("double")
    nb_mois = F.coalesce(
        F.regexp_replace(F.regexp_extract(libelle, r"(?i)sur\s+(\d+(?:[.,]\d+)?)\s*mois", 1), ",", ".").cast("double"),
        F.lit(12.0),
    )
    facteur = (
        # « Mensuel » avec un montant d'au moins 12 000 : c'est en réalité un montant annuel mal étiqueté dans l'offre
        F.when((periode == "mensuel") & (plus_petit >= SALAIRE_MIN_VALIDE), F.lit(1.0))
        .when(periode == "mensuel", nb_mois)
        .when(periode == "horaire", F.lit(float(HEURES_PAR_AN)))
        .when(periode == "hebdomadaire", F.lit(52.0))
        # Période non indiquée : un montant entre 1 000 et 12 000 est lu comme un salaire mensuel (hypothèse)
        .when((periode == "") & plus_petit.between(1000, SALAIRE_MIN_VALIDE), F.lit(12.0))
        .otherwise(F.lit(1.0))                                         # annuel
    )

    def valide(montant):
        return F.when(montant.between(SALAIRE_MIN_VALIDE, SALAIRE_MAX_VALIDE), montant)

    offres = offres.withColumn("salaire_min_annuel", valide(plus_petit * facteur))
    offres = offres.withColumn("salaire_max_annuel", valide(plus_grand * facteur))
    return offres.withColumn(
        "salaire_moyen_annuel",
        F.when(F.col("salaire_min_annuel").isNotNull() & F.col("salaire_max_annuel").isNotNull(),
               F.round((F.col("salaire_min_annuel") + F.col("salaire_max_annuel")) / 2, 0)),
    )


def lire_offres(spark, chemin=CHEMIN_BRUT):
    """Lit les JSON bruts et renvoie une ligne par offre unique, nettoyée et normalisée."""
    dept_vers_region = F.create_map(
        *[x for region, depts in REGIONS.items() for d in depts for x in (F.lit(d), F.lit(region))]
    )
    nom_vers_region = F.create_map(*[x for region in REGIONS for x in (F.lit(sans_accents(region)), F.lit(region))])

    # 1. Lecture : une ligne par offre, avec le fichier d'origine et le secteur de la requête
    brut = spark.read.option("multiLine", "true").json(chemin)
    lignes = brut.select(
        F.input_file_name().alias("fichier"),
        F.col("secteur").alias("secteur_requete"),    # IT / Data / Finance (vide pour la toute première collecte)
        F.explode("resultats").alias("o"),
    )

    # 2. Aplatissement et normalisation
    libelle_contrat = F.col("o.typeContratLibelle")
    nature = F.col("o.natureContrat")
    exp = F.col("o.experienceLibelle")
    plat = lignes.select(
        F.col("o.id").alias("id"),
        F.col("o.intitule").alias("intitule"),
        F.col("o.description").alias("description"),
        F.to_timestamp("o.dateCreation").alias("date_creation"),
        F.to_timestamp("o.dateActualisation").alias("date_actualisation"),
        F.col("o.lieuTravail.libelle").alias("lieu_libelle"),
        F.col("o.romeLibelle").alias("metier_rome"),
        F.col("o.entreprise.nom").alias("entreprise"),
        F.col("o.secteurActiviteLibelle").alias("secteur_employeur"),
        F.col("o.salaire.libelle").alias("salaire_libelle"),
        # Contrat : l'alternance a son propre champ ; on regroupe les durées (« CDD - 12 Mois » -> CDD)
        F.when((F.col("o.alternance") == True) | nature.isin("Contrat apprentissage", "Cont. professionnalisation"),
               "Alternance")
        .when(libelle_contrat.rlike("^CDI"), "CDI")
        .when(libelle_contrat.rlike("^CDD"), "CDD")
        .when(libelle_contrat.rlike("(?i)^Int[ée]rim"), "Intérim")
        .otherwise("Autre").alias("contrat"),
        # Expérience en années : « Débutant accepté » -> 0 ; « 3 An(s) » -> 3 ; « 36 Mois » -> 3 ; sinon inconnue
        F.when(exp.rlike("(?i)d[ée]butant"), F.lit(0.0))
        .when(exp.rlike(r"(\d+)\s*An"), F.regexp_extract(exp, r"(\d+)\s*An", 1).cast("double"))
        .when(exp.rlike(r"(\d+)\s*Mois"), F.round(F.regexp_extract(exp, r"(\d+)\s*Mois", 1).cast("double") / 12, 1))
        .alias("experience_ans"),
        F.regexp_extract(F.col("o.lieuTravail.libelle"), r"^(\d{2,3}|2A|2B)\s*-", 1).alias("departement"),
        "secteur_requete",
        "fichier",
    )
    plat = plat.withColumn("departement", F.when(F.col("departement") == "", None).otherwise(F.col("departement")))

    # 3. Région : par le département, sinon par le nom du lieu
    lieu_norm = F.translate(F.lower("lieu_libelle"), ACCENTS_DE, ACCENTS_VERS)
    plat = plat.withColumn(
        "region",
        F.coalesce(
            dept_vers_region[F.col("departement")],
            nom_vers_region[lieu_norm],
            F.when(lieu_norm.rlike("reunion|guadeloupe|martinique|guyane|mayotte|polynesie|caledonie"), "Outre-mer"),
            F.when(lieu_norm.rlike("luxembourg|belgique|suisse|allemagne|royaume|espagne|italie|pays-bas|irlande|canada|maroc|monaco"),
                   "Étranger"),
            F.lit("Non précisé"),
        ),
    )

    # 4. Dédoublonnage : pour chaque identifiant, on garde la version la plus récente
    secteurs = plat.groupBy("id").agg(F.array_sort(F.collect_set("secteur_requete")).alias("secteurs_requete"))
    fenetre = Window.partitionBy("id").orderBy(F.col("date_actualisation").desc(), F.col("fichier").desc())
    offres = (
        plat.withColumn("rang", F.row_number().over(fenetre))
        .filter("rang = 1")
        .drop("rang", "fichier", "secteur_requete")
        .join(secteurs, "id")
        .withColumn("nb_secteurs", F.size("secteurs_requete"))
    )

    # 5. Un seul secteur par offre (requête, priorité à Data si plusieurs, sinon mots du métier)
    texte = F.translate(F.lower(F.concat_ws(" ", "metier_rome", "intitule")), ACCENTS_DE, ACCENTS_VERS)
    secteur_par_mots = (
        F.when(texte.rlike(r"data|donnee|statisti|business intelligence|power bi|decisionnel|machine learning|analytics|\bbi\b"), "Data")
        .when(texte.rlike(r"financ|comptab|audit|tresor|controleur de gestion|controle de gestion|fiscal|paie|recouvrement|credit"), "Finance")
        .when(texte.rlike(r"informatique|developpe|systeme|reseau|cyber|devops|logiciel|cloud|software|infrastructure"), "IT")
        .otherwise("Autre")
    )
    offres = offres.withColumn(
        "secteur",
        F.when(F.col("nb_secteurs") == 1, F.col("secteurs_requete")[0])
        .when(F.col("nb_secteurs") > 1,
              F.when(F.array_contains("secteurs_requete", "Data"), "Data").otherwise(F.col("secteurs_requete")[0]))
        .otherwise(secteur_par_mots),
    ).withColumn(
        "secteur_methode",
        F.when(F.col("nb_secteurs") == 1, "requête").when(F.col("nb_secteurs") > 1, "règle de priorité").otherwise("mots-clés"),
    )

    # 6. Salaire en montants annuels, et indicateur « débutant accepté »
    offres = _ajouter_salaires(offres)
    offres = offres.withColumn("debutant_accepte", F.col("experience_ans") == 0)
    return offres


def extraire_competences(offres):
    """Une ligne (id, competence, categorie) pour chaque compétence du référentiel citée dans une offre."""
    spark = offres.sparkSession
    texte = F.translate(F.lower(F.concat_ws(" ", "intitule", "description")), ACCENTS_DE, ACCENTS_VERS)

    def motif(mots):
        # Le mot doit être isolé : « sql » ne doit pas être trouvé dans « mysql », ni « java » dans « javascript »
        return r"(?<![a-z0-9+#])(" + "|".join(re.escape(m) for m in mots) + r")(?![a-z0-9+#])"

    trouvees = F.array_compact(
        F.array(*[F.when(texte.rlike(motif(mots)), F.lit(nom)) for nom, (_, mots) in COMPETENCES.items()])
    )
    referentiel = spark.createDataFrame([(nom, cat) for nom, (cat, _) in COMPETENCES.items()], ["competence", "categorie"])
    return offres.select("id", F.explode(trouvees).alias("competence")).join(referentiel, "competence").select(
        "id", "competence", "categorie"
    )
