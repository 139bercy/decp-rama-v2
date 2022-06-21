import wget
import os
import json
import pandas as pd
import xmltodict
import re
import logging
import shutil
pd.options.mode.chained_assignment = None


class SourceProcess:
    def __init__(self, key):

        # Création des variables de source
        self.key = key
        with open("metadata/metadata.json", 'r+') as f:
            self.metadata = json.load(f)
        self.source = self.metadata[self.key]["code"]
        self.format = self.metadata[self.key]["format"]
        self.df = pd.DataFrame()

        # Lavage des dossiers de la source
        self._clean_metadata_folder()

        # Récupération des urls
        self._url_init()

        self.file_name = [f"{self.metadata[self.key]['code']}_{i}" for i in range(len(self.url))]

    def _clean_metadata_folder(self):

        # Lavage des dossiers dans metadata
        logging.info(f"Début du nettoyage de metadata/{self.source}")
        if os.path.exists(f"metadata/{self.source}"):
            shutil.rmtree(f"metadata/{self.source}")
        logging.info(f"Nettoyage metadata/{self.source} OK")

    def _url_init(self):
        logging.info(f"Début de la récupération de la liste des url")
        os.makedirs(f"metadata/{self.source}", exist_ok=True)
        self.cle_api = self.metadata[self.key]["cle_api"]
        url = []
        for i in range(len(self.cle_api)):
            wget.download(f"https://www.data.gouv.fr/api/1/datasets/{self.cle_api[i]}/",
                          f"metadata/{self.source}/metadata_{self.key}_{i}.json")
            with open(f"metadata/{self.source}/metadata_{self.key}_{i}.json", 'r+') as f:
                ref_json = json.load(f)
            ressources = ref_json["resources"]
            url = url + [d["url"] for d in ressources if
                         (d["url"].endswith("xml") or d["url"].endswith("json"))]
        self.metadata[self.key]["url"] = url
        self.url = self.metadata[self.key]["url"]
        logging.info(f"Récupération des url OK")

    def get(self):
        # Lavage des dossiers dans sources
        logging.info(f"Début du nettoyage de sources/{self.source}")
        if os.path.exists(f"sources/{self.source}"):
            shutil.rmtree(f"sources/{self.source}")
        logging.info(f"Nettoyage sources/{self.source} OK")
        # Étape get des url
        logging.info(f"Début du téléchargement : {len(self.url)} fichier(s)")
        self.file_name = [f"{self.metadata[self.key]['code']}_{i}" for i in range(len(self.url))]
        os.makedirs(f"sources/{self.source}", exist_ok=True)
        for i in range(len(self.url)):
            wget.download(self.url[i], f"sources/{self.source}/{self.file_name[i]}.{self.format}")
        logging.info(f"Téléchargement : {len(self.url)} fichier(s) OK")

    def convert(self):
        # suppression des '
        for i in range(len(self.url)):
            file_path = f"sources/{self.source}/{self.file_name[i]}.{self.format}"
            with open(file_path) as file:
                chaine = re.sub("\'", " ", file.read())
            with open(file_path, "w") as file:
                file.write(chaine)
        logging.info(f"Début de convert: mise au format DataFrame de {self.source}")
        if self.format == 'xml':
            li = []
            for i in range(len(self.url)):
                with open(
                        f"sources/{self.source}/{self.file_name[i]}.{self.format}") as xml_file:
                    dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)
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
        logging.info(f"Conversion OK")
        logging.info(f"Nombre de marchés dans {self.source} après convert : {len(self.df)}")

    def fix(self):
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
