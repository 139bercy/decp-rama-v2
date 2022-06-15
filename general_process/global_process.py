import pandas as pd
from dict2xml import dict2xml
import json


class GlobalProcess:
    def __init__(self):
        self.df = pd.DataFrame()

    def merge_all(self):
        print("\n", "Début de l'étape Merge des Dataframes")
        self.df = pd.concat(self.dataframes, ignore_index=True)
        self.df = self.df.reset_index(drop=True)
        print("\n", "Merge OK")

    def drop_duplicate(self):
        # Suppression des doublons
        print("\n", "Début de l'étape Supression des doublons")
        df_str = self.df.astype(str)
        index_to_keep = df_str.drop_duplicates().index.tolist()
        self.df = self.df.iloc[index_to_keep]
        self.df = self.df.reset_index(drop=True)
        print("\n", "Supression OK")

    def export_to_xml(self):
        print("\n", "Début de l'étape Exportation en XML")
        dico = {'marches': [{'marche': {k: v for k, v in m.items() if str(v) != 'nan'}}
                            for m in self.df.to_dict(orient='records')]}
        with open("results/decp.xml", 'w') as f:
            f.write(dict2xml(dico))
        print("\n", "Exportation XML OK")

    def export_to_json(self):
        print("\n", "Début de l'étape Exportation en JSON")
        dico = {'marches': [{k: v for k, v in m.items() if str(v) != 'nan'}
                            for m in self.df.to_dict(orient='records')]}
        with open("results/decp.json", 'w') as f:
            json.dump(dico, f, indent=2, ensure_ascii=False)
        print("\n", "Exportation JSON OK")
