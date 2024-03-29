import pandas as pd
import json
import os
from datetime import date
import pickle
import logging
import boto3
import requests


class GlobalProcess:
    """La classe GlobalProcess est une classe qui définit les étapes de traitement une fois toutes
    les étapes pour toutes les sources effectuées : création des variables de la classe (__init__),
    fusion des sources dans un seul DataFrame (merge_all), suppression des doublons (drop_duplicate)
    et l'exportation des données en json pour publication (export)."""

    def __init__(self):
        """L'étape __init__ crée les variables associées à la classe GlobalProcess : le DataFrame et
        la liste des dataframes des différentes sources."""
        logging.info("------------------------------GlobalProcess------------------------------")
        self.df = pd.DataFrame()
        self.dataframes = []

    def merge_all(self):
        """Étape merge all qui permet la fusion des DataFrames de chacune des sources en un seul."""
        logging.info("  ÉTAPE MERGE ALL")
        logging.info("Début de l'étape Merge des Dataframes")
        self.df = pd.concat(self.dataframes, ignore_index=True)
        self.df = self.df.reset_index(drop=True)
        logging.info("Merge OK")
        logging.info(f"Nombre de marchés dans le DataFrame fusionné après merge : {len(self.df)}")

    def fix_all(self):
        # if df is empty then return
        if len(self.df) == 0:
            logging.warning("Le DataFrame est vide, pas de fix à faire.")
            return
        """Étape fix all qui permet l'uniformisation du DataFrame."""

        logging.info("  ÉTAPE FIX ALL")
        logging.info("Début de l'étape Fix_all du DataFrame fusionné")

        # On met les acheteurs et lieux au bon format
        self.df['acheteur.id'] = self.df['acheteur.id'].astype(str)
        self.df['lieuExecution.code'] = self.df['lieuExecution.code'].astype(str)

        # Suppression des colonnes inutiles
        self.df = self.df.drop('dateTransmissionDonneesEtalab', axis=1)
        # Format des dates
        date_columns = ['dateNotification', 'datePublicationDonnees',
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
        logging.info(f"Nombre de marchés dans le DataFrame fusionné après merge : {len(self.df)}")
        # DureeMois doit être un float
        self.df['dureeMois'] = self.df['dureeMois'].apply(lambda x: 0 if x == '' or
                                                          str(x) in ['nan', 'None'] else x)
        # Type de contrat qui s'étale sur deux colonnes, on combine les deux et garde _type qui est l'appelation dans Ramav1
        dict_mapping = {"MARCHE_PUBLIC": "Marché", "CONTRAT_DE_CONCESSION":"Contrat de concession"}
        bool_nan_type = self.df.loc[:, "_type"].isna()
        cols_to_drop = []
        if "typeContrat" in self.df.columns:  # Dans le cas où typeContrat n'existe pas, on ne fait rien
            self.df.loc[bool_nan_type, "_type"] = self.df.loc[bool_nan_type, "typeContrat"].map(dict_mapping)
            cols_to_drop.append("typeContrat") # On supprime donc typeContrat qui est maintenant vide
        if "ReferenceAccordCadre" in self.df.columns: # Dans le cas où ReferenceAccordCadre n'existe pas, on ne fait rien
            cols_to_drop.append("ReferenceAccordCadre")
        # ReferenceAccordCadre n'a que 6 valeurs non nul sur 650k lignes et en plus cette colonne n'existe pas dans v1.
        self.df = self.df.drop(cols_to_drop, axis=1)
        # S'il y a des Nan dans les modifications, on met une liste vide pour coller au format du v1
        mask_modifications_nan = self.df.loc[:, "modifications"].isnull()
        self.df.modifications.loc[mask_modifications_nan] = self.df.modifications.loc[mask_modifications_nan].apply(lambda x: [])
        # Gestion des multiples modifications  ===> C'est traité dans la partie gestion de la version flux. On va garder cette manière de faire, mais il faut une autre solution pour les unashable type.
        #col_to_normalize = "modifications"
        #mask_multiples_modifications = self.df.modifications.apply(lambda x:len(x)>1)
        #self.df.loc[mask_multiples_modifications, col_to_normalize] = self.df.loc[mask_multiples_modifications, col_to_normalize].apply(concat_modifications).apply(trans)
        
        #mask_modif = self.df.modifications.apply(len)>0
        #self.df.loc[mask_modif, "modifications"] = self.df.loc[mask_modif, "modifications"].apply(remove_titulaire_key_in_modif)

    def drop_duplicate(self):
        # if df is empty then return
        if len(self.df) == 0:
            logging.warning(f"Le DataFrame est vide, impossible de supprimer les doublons, source : {self.source}")
            return
        """L'Étape drop duplicate supprime les duplicats purs après avoir supprimé les espaces et
        convertis l'ensemble du DataFrame en string."""
        # Suppression des doublons
        logging.info("Début de l'étape Suppression des doublons")
        self.df.sort_values(by="source", inplace=True) # Pourquoi ? La partie métier (Martin Douysset) a demandé à ce qu'en cas de doublon sur plusieurs sources, ceux de l'AIFE
        df_modif = self.df[self.df.modifications.apply(len)>0] # Les règles de dédoublonnages diffèrent dans ces cas là.
        df_nomodif = self.df[self.df.modifications.apply(len)==0]
        # Suppression des doublons
        df_nomodif_str = df_nomodif.astype(str) # Pour avoir des objets dedoublonnables
        feature_doublons = ["objet", "acheteur.id", "titulaires", "dateNotification", "montant"]
        index_to_keep_nomodif = df_nomodif_str.drop_duplicates(subset=feature_doublons).index.tolist()
        df_modif_str = df_modif.astype(str)
        index_to_keep_modif = df_modif_str.drop_duplicates().index.tolist() # Dans le cas des modifs, un dédoublonnage dure.
        self.df = pd.concat([df_nomodif.loc[index_to_keep_nomodif, :], df_modif.loc[index_to_keep_modif, :]])
        self.df = self.df.reset_index(drop=True)
        logging.info("Suppression OK")
        logging.info(f"Nombre de marchés dans Df après suppression des doublons strictes : {len(self.df)}")

    def export(self):
        # if df is empty then return
        if len(self.df) == 0:
            logging.warning(f"Le DataFrame est vide, impossible d'exporter, source : {self.source}")
            return
        """Étape exportation des résultats au format json et xml dans le dossier /results"""
        logging.info("  ÉTAPE EXPORTATION")
        logging.info("Début de l'étape Exportation en XML")
        """
        dico = {'marches': [{'marche': {k: v for k, v in m.items() if str(v) != 'nan'}}
                            for m in self.df.to_dict(orient='records')]}
        with open("results/decp.xml", 'w') as f:
            f.write(dict2xml(dico))
        xml_size = os.path.getsize(r'results/decp.xml')
        logging.info("Exportation XML OK")
        logging.info(f"Taille de decp.xml : {xml_size}")
        """
        logging.info("Début de l'étape Exportation en JSON")
        dico = {'marches': [{k: v for k, v in m.items() if str(v) != 'nan'}
                            for m in self.df.to_dict(orient='records')]}
        with open('dico.pkl', 'wb') as f:
            pickle.dump(dico, f)
        for marche in dico['marches']:
            if 'titulaires' in marche.keys() and marche['titulaires'] is not None and len(
                    marche['titulaires']) > 0:
                if type(marche['titulaires'][0]['titulaire']) == list:
                    marche['titulaires'] = marche['titulaires'][0]['titulaire']
                else:
                    marche['titulaires'] = [marche['titulaires'][0]['titulaire']]

            # Retrait car on a géré les multiples modifications. Donc on n'y touche plus
            
            if 'modifications' in marche.keys() and marche['modifications'] is not None and len(
                    marche['modifications']) > 0:
                
                if type(marche['modifications'][0]['modification']) == list:
                        marche['modifications'] = marche['modifications'][0]['modification']
                else:
                    marche['modifications'] = [marche['modifications'][0]['modification']]
            """
            A servit à un moment pour régler un soucis avec concessionaire mais la clef à disparue depuis
            if 'concessionnaires' in marche.keys() and marche[
                    'concessionnaires'] is not None and len(marche['concessionnaires']) > 0:
                if type(marche['concessionnaires'][0]['concessionnaire']) == list:
                    marche['concessionnaires'] = marche['concessionnaires'][0]['concessionnaire']
                else:
                    marche['concessionnaires'] = [marche['concessionnaires'][0]['concessionnaire']]
            """
        path_result = "results/decpv2.json"
        os.makedirs("results", exist_ok=True)
        with open(path_result, 'w') as f:
            json.dump(dico, f, indent=2, ensure_ascii=False)
        json_size = os.path.getsize(path_result)
        logging.info("Exportation JSON OK")
        logging.info(f"Taille de decpv2.json : {json_size}")

    def upload_s3(self):
        """
        Cette fonction exporte decpv2 sur le S3 decp.
        """
        ACCESS_KEY = os.environ.get("ACCESS_KEY")
        SECRET_KEY = os.environ.get("SECRET_KEY")
        ENDPOINT_S3 = os.environ.get("ENDPOINT_S3")
        BUCKET_NAME = os.environ.get("BUCKET_NAME")
        session = boto3.session.Session()
        client = session.client(
            service_name='s3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="gra",
            endpoint_url="https://"+str(ENDPOINT_S3)
        )
        client.upload_file(os.path.join("results", "decpv2.json"), BUCKET_NAME, "data/decpv2.json")

    def upload_datagouv(self):
        """
        Cette fonction exporte decpv2.json sur data.gouv.fr
        """
        # read info from config.son
        with open("config.json", "r") as f:
            config = json.load(f)
            api = config["url_api"]
            dataset_id = config["dataset_id"]
            resource_id_json = config["resource_id_json"]

        url = f"{api}/datasets/{dataset_id}/resources/{resource_id_json}/upload/"

        headers = {
            "X-API-KEY": os.environ.get("DATA_GOUV_API_KEY")
        }

        files = {
            "file": (f"decpv2.json", open(f"results/decpv2.json", "rb"))
        }

        response = requests.post(url, headers=headers, files=files)

        print(response.status_code)
