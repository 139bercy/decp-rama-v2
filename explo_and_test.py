import pandas as pd
import wget
import xmltodict
import os

# %% Importation des metadatas des sources
metadata = pd.read_json('metadata.json')
# %%


def get_init():
    # Supprimer tous les fichiers présents dans sources/
    for root, dirs, files in os.walk("sources/", topdown=False):
        for name in files:
            if name != '.gitignore':
                os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))


def get_pes():
    os.makedirs(f"sources/{metadata['code'][0]}", exist_ok=True)
    URL = metadata["url"][0]
    wget.download(URL, f"sources/{metadata['code'][0]}/{metadata['code'][0]}.xml")


def get_aws():
    os.makedirs(f"sources/{metadata['code'][1]}", exist_ok=True)
    for i in range(5):
        URL = metadata["url"][1][i]
        wget.download(URL, f"sources/{metadata['code'][1]}/{metadata['code'][1]}_{2018+i}.xml")


def convert_pes():
    with open(f"sources/{metadata['code'][0]}/{metadata['code'][0]}.xml") as fd:
        doc = xmltodict.parse(fd.read())
    df_pes = pd.DataFrame.from_dict(doc['marches']['marche'])
    return df_pes


def convert_aws():
    return


def fix_pes():
    return


def fix_aws():
    return


def merge_all():
    df_merged = pd.concat(df_to_concat)
    return df_merged


# %% GET
# %%% GET INIT
get_init()

# %%% GET PES/AWS
get_pes()
get_aws()

# %% CONVERT
# %%% CONVERT PES/AWS
df_pes = convert_pes()
df_aws = convert_pes()

# %% FIX
# %%% FIX PES/AWS
fix_pes()
fix_aws()

# %% MERGE ALL
# %%%
df_to_concat = []
df_to_concat.append(df_pes)
df_to_concat.append(df_aws)
# %%%
merge_all()
