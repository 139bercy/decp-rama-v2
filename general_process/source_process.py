import wget
import os
import json
import pandas as pd
import xmltodict
import re
import logging


class SourceProcess:
    def __init__(self, id_int):
        self.id_int = id_int
        self.metadata = pd.read_json('metadata/metadata.json')
        self.url = self.metadata["url"][self.id_int]
        self.file_name = [f"{self.metadata['code'][self.id_int]}_{i}" for i in range(len(self.url))]
        self.source = self.metadata["code"][self.id_int]
        self.format = self.metadata["format"][self.id_int]
        self.df = pd.DataFrame()

    def get(self):
        logging.info(" ")
        logging.info(" ")
        logging.info(" ")
        logging.info(f"Début du téléchargement : {len(self.url)} fichier(s) pour la source {self.source}")
        os.makedirs(f"sources/{self.source}", exist_ok=True)
        for i in range(len(self.url)):
            logging.info(f"{i+1}/{len(self.url)} : {self.file_name[i]}")
            wget.download(self.url[i], f"sources/{self.source}/{self.file_name[i]}.{self.format}")

    def convert(self):
        logging.info(" ")
        # suppression des '
        for i in range(len(self.url)):
            with open(
                    f"sources/{self.source}/{self.file_name[i]}.{self.format}") as file:
                chaine = re.sub("\'", " ", file.read())
            with open(
                    f"sources/{self.source}/{self.file_name[i]}.{self.format}", "w") as file:
                file.write(chaine)
        logging.info(f"Début de convert: mise au format DataFrame de {self.source}")
        if self.format == 'xml':
            li = []
            for i in range(len(self.url)):
                with open(
                        f"sources/{self.source}/{self.file_name[i]}.{self.format}") as xml_file:
                    dico = xmltodict.parse(xml_file.read(), dict_constructor = dict)
                    df = pd.DataFrame.from_dict(dico['marches']['marche'])
                li.append(df)
            df = pd.concat(li)
            df = df.reset_index(drop=True)
            self.df = df
        elif self.format == 'json':
            li = []
            for i in range(len(self.url)):
                with open(
                        f"sources/{self.source}/{self.file_name[i]}.{self.format}") as json_file:
                    dico = json.load(json_file)
                li.append(pd.DataFrame.from_dict(dico['marches']))
            df = pd.concat(li)
            df = df.reset_index(drop=True)
            self.df = df
        logging.info(f"Conversion de {self.source} OK")
        logging.info(f"Nombre de marchés dans {self.source} après convert : {len(self.df)}")

    def fix(self):
        logging.info(" ")
        logging.info(f"Début de fix: Ajout de la colonne source et suppression des duplicatas de {self.source}")
        # Ajout de source
        self.df = self.df.assign(source=self.source)
        # Suppression des doublons
        df_str = self.df.astype(str)
        index_to_keep = df_str.drop_duplicates().index.tolist()
        self.df = self.df.iloc[index_to_keep]
        self.df = self.df.reset_index(drop=True)
        logging.info(f"Fix de {self.source} OK")
        logging.info(f"Nombre de marchés dans {self.source} après fix : {len(self.df)}")
