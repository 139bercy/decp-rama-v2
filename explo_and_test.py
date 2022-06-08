import pandas as pd
import wget
import xmltodict
from datetime import datetime
import os

# %% Importation des metadatas des sources
metadata = pd.read_json('sources/metadata.json')
# %%


def get_pes():
    os.makedirs(f"sources/{metadata['code'][0]}", exist_ok=True)
    URL = metadata["url"][0]
    wget.download(URL, f"sources/{metadata['code'][0]}/{metadata['code'][0]}.xml")


def get_aws():
    os.makedirs(f"sources/{metadata['code'][1]}", exist_ok=True)
    for i in range(5):
        URL = metadata[f"url{2018+i}"][1]
        wget.download(URL, f"sources/{metadata['code'][1]}/{metadata['code'][1]}_{2018+i}.xml")


# %%
get_pes()
get_aws()

# %% Importation des données sous format dataframe
start_time = datetime.now()
with open('dgfip-pes-decp.xml') as fd:
    doc = xmltodict.parse(fd.read())

df_base = pd.DataFrame.from_dict(doc['marches']['marche'])
end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))
# %% Statistiques descriptives pour savoir le nombre de modifications et titulaires maximums
modicount = df_base["modifications"].apply(lambda x: None if x is None else len(x["modification"]))
titucount = df_base["titulaires"].apply(lambda x: None if x is None else len(x["titulaire"]))

# %% CLEAN 1 : supression des colonnes acheteur et lieuExecution
df_clean_1 = df_base
df_clean_1["acheteur_id"] = df_clean_1["acheteur"].apply(lambda x: x["id"])
df_clean_1["acheteur_nom"] = df_clean_1["acheteur"].apply(lambda x: x["nom"])

df_clean_1["lieuExecution_code"] = df_clean_1["lieuExecution"].apply(lambda x: x["code"])
df_clean_1["lieuExecution_typeCode"] = df_clean_1["lieuExecution"].apply(lambda x: x["typeCode"])
df_clean_1["lieuExecution_nom"] = df_clean_1["lieuExecution"].apply(lambda x: x["nom"])

df_clean_1 = df_clean_1.drop(["acheteur", "lieuExecution"], axis=1)
# %% CLEAN 2 : supression de la colonne titulaires en .explode()
