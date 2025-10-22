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
collection_target = "citerne-daily-interpol"

db = client["abbaye11"]
coll_source = db[collection_source]
coll_target = db[collection_target]



# === Lecture des données depuis la collection source ===
releves = list(coll_source.find({}, {
    "_id": 0,
    "ActualVolume": 1,
    "LastUpdate": 1
}))

# Nettoyage et conversion
for r in releves:
    r["ActualVolume"] = float(r["ActualVolume"]) * 2  # ⚠️ Doublement pour 2 cuves
    r["LastUpdate"] = pd.to_datetime(r["LastUpdate"])

df = pd.DataFrame(releves)

# Ajouter une colonne "date_jour" pour regrouper les relevés par jour
df["date_jour"] = df["LastUpdate"].dt.date

# Garder le dernier relevé de chaque jour
df_daily = df.sort_values("LastUpdate").groupby("date_jour").last().reset_index()

# Réindexer la série par dates continues
df_daily.set_index("date_jour", inplace=True)

# Créer une série de dates complètes
full_date_range = pd.date_range(start=df_daily.index.min(), end=df_daily.index.max(), freq="D")

# Réindexer avec interpolation
df_complete = df_daily.reindex(full_date_range)

# Interpoler les volumes
df_complete["ActualVolume"] = df_complete["ActualVolume"].interpolate(method="linear")

# Conserver les dates et flag interpolé
df_complete["is_interpolated"] = df_complete["LastUpdate"].isna()
df_complete["LastUpdate"] = df_complete["LastUpdate"].fillna(
    df_complete.index.to_series().apply(lambda d: datetime.combine(d.date(), datetime.min.time()))
)

# Recalcul de la consommation
df_complete["consommation_litres"] = df_complete["ActualVolume"].shift(1) - df_complete["ActualVolume"]
df_complete["consommation_litres"] = df_complete["consommation_litres"].apply(lambda x: x if x > 0 else 0)

# Conversion en documents MongoDB
docs_to_insert = []
for date, row in df_complete.iterrows():
    doc = {
        "date": datetime.combine(date.date(), datetime.min.time()),
        "volume_total_litres": round(row["ActualVolume"], 1),
        "consommation_litres": round(row["consommation_litres"], 1) if pd.notna(row["consommation_litres"]) else None,
        "last_update": row["LastUpdate"].to_pydatetime(),
        "interpole": bool(row["is_interpolated"]),
    }
    docs_to_insert.append(doc)

# Insertion dans la collection cible
if docs_to_insert:
    coll_target.delete_many({})
    coll_target.insert_many(docs_to_insert)
    print(f"{len(docs_to_insert)} documents (avec interpolation) insérés dans '{collection_target}'")
else:
    print("Aucun document à insérer.")