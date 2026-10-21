"""
Exploration des offres collectées dans data/raw.
Sert à décider comment calculer les indicateurs. Lecture seule : ne modifie aucun fichier.
Utilise uniquement la bibliothèque standard de Python (rien à installer).
"""
import glob
import json
import sys
from collections import Counter

# Évite les erreurs d'affichage des accents dans le terminal Windows
sys.stdout.reconfigure(encoding="utf-8")

# 1. Lire tous les fichiers JSON de data/raw et regrouper les offres
offres = []
for chemin in glob.glob("data/raw/*.json"):
    with open(chemin, encoding="utf-8") as f:
        offres.extend(json.load(f).get("resultats", []))

# 2. Dédoublonner : une même offre peut ressortir pour plusieurs mots-clés
uniques = list({o["id"]: o for o in offres}.values())
print(f"Offres lues : {len(offres)} | offres uniques : {len(uniques)}")

# 3. Période couverte par les dates de création
dates = sorted(o["dateCreation"][:10] for o in uniques if o.get("dateCreation"))
print(f"dateCreation : de {dates[0]} à {dates[-1]}")


# 4. Répartition de quelques champs clés
def compter(titre, extracteur, n=8):
    compteur = Counter(extracteur(o) for o in uniques)
    print(f"\n{titre}")
    for valeur, nombre in compteur.most_common(n):
        print(f"  {nombre:4d}  {valeur}")


compter("typeContratLibelle", lambda o: o.get("typeContratLibelle"))
compter("natureContrat", lambda o: o.get("natureContrat"))
compter("alternance", lambda o: o.get("alternance"))
compter("romeLibelle (métier)", lambda o: o.get("romeLibelle"))
compter("secteurActiviteLibelle (secteur de l'employeur)", lambda o: o.get("secteurActiviteLibelle"))
compter("experienceLibelle", lambda o: o.get("experienceLibelle"))


# 5. Champs souvent vides ou imbriqués
def libelle_salaire(offre):
    salaire = offre.get("salaire")
    return salaire.get("libelle") if isinstance(salaire, dict) else None


avec_salaire = sum(1 for o in uniques if libelle_salaire(o))
print(f"\nOffres avec un salaire renseigné : {avec_salaire} sur {len(uniques)}")
print("Exemple de lieuTravail :", uniques[0].get("lieuTravail"))
