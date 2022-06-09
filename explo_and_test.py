import pandas as pd
import wget
import xmltodict
import os
import json

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
    n = len(metadata['url'][1])
    for i in range(n):
        URL = metadata["url"][1][i]
        wget.download(URL, f"sources/{metadata['code'][1]}/{metadata['code'][1]}_{2018+i}.json")


def convert_pes():
    with open(f"sources/{metadata['code'][0]}/{metadata['code'][0]}.xml") as fd:
        doc = xmltodict.parse(fd.read())
    df_pes = pd.DataFrame.from_dict(doc['marches']['marche'])
    return df_pes


def convert_aws():
    start_time = datetime.now()
    n = len(metadata['url'][1])
    li_df_aws = []
    for i in range(n):
        with open(
                f"sources/{metadata['code'][1]}/{metadata['code'][1]}_{2018+i}.json") as json_file:
            dico = json.load(json_file)
        li_df_aws.append(pd.DataFrame.from_dict(dico['marches']))
    df_aws = pd.concat(li_df_aws)
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))
    return df_aws


def fix_pes(df):
    # Ajout de la source
    df = df.assign(source=f"{metadata['code'][0]}")
    # Conversion des OrderedDict en dict dans acheteur, lieuExecution, titulaires et modifications
    df['acheteur'] = df['acheteur'].apply(lambda x: json.loads(json.dumps(x)))
    df['lieuExecution'] = df['lieuExecution'].apply(lambda x: json.loads(json.dumps(x)))
    df['titulaires'] = df['titulaires'].apply(
        lambda x: json.loads(json.dumps(x))['titulaire'])
    df['titulaires'] = df['titulaires'].apply(
        lambda x: x if x is None or type(x) == list else [x])
    df['modifications'] = df['modifications'].apply(
        lambda x: x if x is None else json.loads(json.dumps(x))['modification'])
    df['modifications'] = df['modifications'].apply(
        lambda x: x if type(x) == list else [] if x is None else [x])
    return df


def fix_aws(df):
    df = df.assign(source=f"{metadata['code'][1]}")
    df['dureeMois'] = df['dureeMois'].astype(str)
    df['montant'] = df['montant'].astype(str)
    return df


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
df_pes = fix_pes(df_pes)
df_aws = fix_aws(df_aws)

# %% MERGE ALL
# %%%
df_to_concat = []
df_to_concat.append(df_pes)
df_to_concat.append(df_aws)
# %%%
merge_all()
