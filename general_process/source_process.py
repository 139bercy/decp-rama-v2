import wget
import os
import json
import pandas as pd
import xmltodict
import re


class SourceProcess:
    def __init__(self):
        self.metadata = pd.read_json('metadata.json')
        self.url = self.metadata["url"][self.id]
        self.file_name = [f"{self.metadata['code'][self.id]}_{i}" for i in range(len(self.url))]
        self.source = self.metadata["code"][self.id]
        self.format = self.metadata["format"][self.id]
        self.df = pd.DataFrame()

    def get(self):
        print("\n",
              f"Début du téléchargement de {len(self.url)} fichier(s) pour la source {self.source}")
        os.makedirs(f"sources/{self.source}", exist_ok=True)
        for i in range(len(self.url)):
            print("\n", f"{i+1}/{len(self.url)} : {self.file_name[i]}")
            wget.download(self.url[i], f"sources/{self.source}/{self.file_name[i]}.{self.format}")

    def convert(self):
        # suppression des '
        for i in range(len(self.url)):
            with open(
                    f"sources/{self.source}/{self.file_name[i]}.{self.format}") as file:
                str = re.sub("\'", " ", file.read())
            with open(
                    f"sources/{self.source}/{self.file_name[i]}.{self.format}", "w") as file:
                file.write(str)
        print("\n", f"Début de convert: mise au format DataFrame de {self.source}")
        if self.format == 'xml':
            li = []
            for i in range(len(self.url)):
                with open(
                        f"sources/{self.source}/{self.file_name[i]}.{self.format}") as xml_file:
                    dico = xmltodict.parse(xml_file.read())
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
        print("\n", f"Conversion de {self.source} OK")

    def fix(self):
        print("\n", "Début de fix : Ajout de la colonne source et supression des duplicats de " +
              f"{self.source}")
        # Ajout de source
        self.df = self.df.assign(source=self.source)
        # Suppression des doublons
        df_str = self.df.astype(str)
        index_to_keep = df_str.drop_duplicates().index.tolist()
        self.df = self.df.iloc[index_to_keep]
        self.df = self.df.reset_index(drop=True)
        print("\n", f"Fix de {self.source} OK")
