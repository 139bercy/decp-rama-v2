import pandas as pd
from dict2xml import dict2xml
import json
import os
from datetime import date
import logging


class GlobalProcess:
    def __init__(self):
        self.df = pd.DataFrame()
        self.dataframes = []

    def merge_all(self):
        logging.info("Début de l'étape Merge des Dataframes")
        self.df = pd.concat(self.dataframes, ignore_index=True)
        self.df = self.df.reset_index(drop=True)
        logging.info("Merge OK")
        logging.info(f"Nombre de marchés dans le DataFrame fusionné après merge : {len(self.df)}")

    def fix_all(self):
        logging.info("Début de l'étape Fix_all du DataFrame fusionné")
        date_columns = ['dateNotification', 'datePublicationDonnees', 'dateTransmissionDonneesEtalab',
                        'dateDebutExecution']
        for s in date_columns:
            self.df[s] = self.df[s].apply(str)
            self.df[s] = self.df[s].apply(lambda x:
                                          x.replace('+', '-') if str(x) != 'nan' else x)
            self.df[s] = \
                self.df[s].apply(lambda x:
                                 date(int(float(x.split("-")[0])), int(float(x.split("-")[1])),
                                      int(float(x.split("-")[2]))).isoformat()
                                 if str(x) != 'nan' and len(x.split("-")) >= 3 else x)
        logging.info("Début de l'étape Fix_all du DataFrame fusionné")
        logging.info(f"Nombre de marchés dans le DataFrame fusionné après merge : {len(self.df)}")

    def drop_duplicate(self):
        # Suppression des doublons
        logging.info("Début de l'étape Suppression des doublons")
        df_str = self.df.astype(str)
        for c in df_str.columns:
            df_str[c] = df_str[c].str.replace(' ', '')
        df_str = df_str.drop(["source"], axis=1)
        index_to_keep = df_str.drop_duplicates().index.tolist()
        self.df = self.df.iloc[index_to_keep]
        self.df = self.df.reset_index(drop=True)
        logging.info("Suppression OK")
        logging.info(f"Nombre de marchés dans le DataFrame fusionné après suppression des doublons : {len(self.df)}")

    def export_to_xml(self):
        logging.info("Début de l'étape Exportation en XML")
        dico = {'marches': [{'marche': {k: v for k, v in m.items() if str(v) != 'nan'}}
                            for m in self.df.to_dict(orient='records')]}
        with open("results/decp.xml", 'w') as f:
            f.write(dict2xml(dico))
        xml_size = os.path.getsize(r'results/decp.xml')
        logging.info("Exportation XML OK")
        logging.info(f"Taille de decp.xml : {xml_size}")

    def export_to_json(self):
        logging.info("Début de l'étape Exportation en JSON")
        dico = {'marches': [{k: v for k, v in m.items() if str(v) != 'nan'}
                            for m in self.df.to_dict(orient='records')]}
        with open("results/decp.json", 'w') as f:
            json.dump(dico, f, indent=2, ensure_ascii=False)
        json_size = os.path.getsize(r'results/decp.json')
        logging.info("Exportation JSON OK")
        logging.info(f"Taille de decp.json : {json_size}")
