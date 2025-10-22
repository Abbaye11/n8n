from pymongo import MongoClient
from urllib.parse import quote_plus
import pandas as pd
from datetime import datetime

username = "seb"
password = "Seb%110978"  # Mot de passe avec caractères spéciaux

# Encodage du mot de passe
username_encoded = quote_plus(username)
password_encoded = quote_plus(password)

# Construction de l’URI MongoDB
uri = f"mongodb://{username_encoded}:{password_encoded}@mongodb.abbaye11.ch:27017/"
# Connexion MongoDB
client = MongoClient(uri)  # à adapter si besoin


collection_source = "citerne-reports"
collection_target = "citerne-daily"

db = client["abbaye11"]
coll_source = db[collection_source]
coll_target = db[collection_target]



# === Lecture des données depuis la collection source ===
releves = list(coll_source.find({}, {
    "_id": 0,
    "ActualVolume": 1,
    "LastUpdate": 1
}))

for r in releves:
    r["ActualVolume"] = float(r["ActualVolume"]) * 2
    r["LastUpdate"] = pd.to_datetime(r["LastUpdate"])

# Conversion en DataFrame
df = pd.DataFrame(releves)

# Ajout d'une colonne "date_jour" sans l'heure pour grouper par jour
df["date_jour"] = df["LastUpdate"].dt.date

# On garde uniquement le dernier relevé de chaque jour
df_daily = df.sort_values("LastUpdate").groupby("date_jour").last().reset_index()

# Calcul de la consommation en litres par différence de volume (jour précédent - jour actuel)
df_daily["consommation_litres"] = df_daily["ActualVolume"].shift(1) - df_daily["ActualVolume"]

# On ignore les jours où le volume augmente (remplissage ou anomalie)
df_daily["consommation_litres"] = df_daily["consommation_litres"].apply(lambda x: x if x > 0 else 0)

# Conversion en documents pour MongoDB
docs_to_insert = []
for _, row in df_daily.iterrows():
    doc = {
        "date": datetime.combine(row["date_jour"], datetime.min.time()),  # format date Mongo
        "volume_total_litres": round(row["ActualVolume"], 1),
        "consommation_litres": round(row["consommation_litres"], 1) if pd.notna(row["consommation_litres"]) else None,
        "last_update": row["LastUpdate"].to_pydatetime()  # format datetime Mongo
    }
    docs_to_insert.append(doc)
# Insertion dans la collection cible
if docs_to_insert:
    coll_target.delete_many({})  # 🔁 Optionnel : nettoyer avant insertion
    coll_target.insert_many(docs_to_insert)
    print(f"{len(docs_to_insert)} documents insérés dans '{collection_target}'")
else:
    print("Aucun document à insérer.")

# Affichage en console
print("\nConsommation journalière (en litres):\n")
for _, row in df_daily.iterrows():
    date_str = row["date_jour"].strftime("%Y-%m-%d")
    volume = row["ActualVolume"]
    conso = row["consommation_litres"]
    if pd.isna(conso):
        print(f"{date_str} - Volume: {volume:.1f} L - Consommation: --")
    else:
        print(f"{date_str} - Volume: {volume:.1f} L - Consommation: {conso:.1f} L")
