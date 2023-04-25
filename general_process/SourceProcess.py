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
    """La classe SourceProcess est une classe abstraite qui sert de parent à chaque classe enfant de
    sources. Elle sert à définir le cas général des étapes de traitement d'une source : création des
    variables de classe (__init__), nettoyage des dossiers de la source (_clean_metadata_folder),
    récupération des URLs (_url_init), get, convert et fix."""

    def __init__(self, key):
        """L'étape __init__ crée les variables associées à la classe SourceProcess : key, source,
        format, df, file_name, url, cle_api et metadata."""
        logging.info("  ÉTAPE INIT")
        self.key = key
        with open("metadata/metadata.json", 'r+') as f:
            self.metadata = json.load(f)
        self.source = self.metadata[self.key]["code"]
        self.format = self.metadata[self.key]["format"]
        self.url_source = self.metadata[self.key]["url_source"]
        self.df = pd.DataFrame()

        # Lavage des dossiers de la source
        self._clean_metadata_folder()

        # Récupération des urls
        self._url_init()

        self.file_name = [f"{self.metadata[self.key]['code']}_{i}" for i in range(len(self.url))]

    def _clean_metadata_folder(self):
        """La fonction _clean_metadata_folder permet le nettoyage de /metadata/{self.source}"""
        # Lavage des dossiers dans metadata
        logging.info(f"Début du nettoyage de metadata/{self.source}")
        if os.path.exists(f"metadata/{self.source}"):
            shutil.rmtree(f"metadata/{self.source}")
        logging.info(f"Nettoyage metadata/{self.source} OK")

    def _url_init(self):
        """_url_init permet la récupération de l'ensemble des url des fichiers qui doivent être
        téléchargés pour une source. Ces url sont conservés dans self.metadata, le dictionnaire
        correspondant à la source."""
        logging.info("Début de la récupération de la liste des url")
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
        logging.info("Récupération des url OK")

    def get(self):
        """Étape get qui permet le lavage du dossier sources/{self.source} et la récupération de
        l'ensemble des fichiers présents sur chaque url."""
        logging.info("  ÉTAPE GET")
        # Lavage des dossiers dans "sources"
        logging.info(f"Début du nettoyage de sources/{self.source}")
        if os.path.exists(f"sources/{self.source}"):
            shutil.rmtree(f"sources/{self.source}")
        logging.info(f"Nettoyage sources/{self.source} OK")
        # Étape get des url
        logging.info(f"Début du téléchargement : {len(self.url)} fichier(s)")
        self.file_name = [f"{self.metadata[self.key]['code']}_{i}" for i in range(len(self.url))]
        os.makedirs(f"sources/{self.source}", exist_ok=True)
        for i in range(len(self.url)):
            try:
                wget.download(self.url[i], f"sources/{self.source}/{self.file_name[i]}.{self.format}")
            except:
                print("Problème de téléchargement du fichier ", self.url[i])
        logging.info(f"Téléchargement : {len(self.url)} fichier(s) OK")

    def convert(self):
        """Étape de conversion des fichiers qui supprime les ' et concatène les fichiers présents
        dans {self.source} dans un seul DataFrame"""
        logging.info("  ÉTAPE CONVERT")
        # suppression des '
        count = 0
        repertoire_source = f"sources/{self.source}"
        for path in os.listdir(repertoire_source):
            if os.path.isfile(os.path.join(repertoire_source, path)):
                count += 1
        for i in range(count):
            file_path = f"sources/{self.source}/{self.file_name[i]}.{self.format}"
            file_exist = os.path.exists(file_path)
            if file_exist:
                with open(file_path) as file:
                    chaine = re.sub("\'", " ", file.read())
                with open(file_path, "w") as file:
                    file.write(chaine)
            else:
                print(f" Fichier {file_path} n existe pas.")
        if count != len(self.url):
            logging.warning("Nombre de fichiers en local inégal au nombre d'url trouvé")
        logging.info(f"Début de convert: mise au format DataFrame de {self.source}")
        if self.format == 'xml':
            li = []
            for i in range(count):
                try :
                    with open(
                            f"sources/{self.source}/{self.file_name[i]}.{self.format}", encoding="utf-8") as xml_file:
                        dico = xmltodict.parse(xml_file.read(), dict_constructor=dict)

                    if dico['marches'] is not None:
                        df = pd.DataFrame.from_dict(dico['marches']['marche'])
                        li.append(df)
                    else:  # cas presque nul
                        logging.warning(f"Le fichier {self.file_name[i]} est vide, il est ignoré")
                except:
                    logging.error(f"Le fichier {self.file_name[i]} de la source {self.source}, n'est pas au format xml, il est ignoré")

            if len(li) != 0:
                df = pd.concat(li)
                df = df.reset_index(drop=True)
            else:
                # create empty dataframe
                df = pd.DataFrame()
            self.df = df
        elif self.format == 'json':
            li = []
            for i in range(count):
                try:
                    with open(
                            f"sources/{self.source}/{self.file_name[i]}.{self.format}") as json_file:
                        dico = json.load(json_file)
                        li.append(pd.DataFrame.from_dict(dico['marches']))
                except:
                    print(f" Le fichier {self.source} est introuvable")
            df = pd.concat(li)
            df = df.reset_index(drop=True)
            self.df = df
        logging.info("Conversion OK")
        logging.info(f"Nombre de marchés dans {self.source} après convert : {len(self.df)}")

    def fix(self):
        """Étape fix qui crée la colonne source dans le DataFrame et qui supprime les doublons
        purs."""
        logging.info("  ÉTAPE FIX")
        logging.info(f"Début de fix: Ajout source et suppression des doublons de {self.source}")
        # Ajout de source
        self.df = self.df.assign(source=self.source)
        # Suppression des doublons
        df_str = self.df.astype(str)
        index_to_keep = df_str.drop_duplicates().index.tolist()
        self.df = self.df.iloc[index_to_keep]
        self.df = self.df.reset_index(drop=True)
        logging.info(f"Fix de {self.source} OK")
        logging.info(f"Nombre de marchés dans {self.source} après fix : {len(self.df)}")
