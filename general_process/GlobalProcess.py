import pandas as pd
from dict2xml import dict2xml
import json
import os
from datetime import date
import pickle
import logging


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
        """Étape fix all qui permet l'uniformisation du DataFrame."""
        logging.info("  ÉTAPE FIX ALL")
        logging.info("Début de l'étape Fix_all du DataFrame fusionné")
        # On met les acheteurs et lieux au bon format
        for x in self.df['acheteur']:
            if type(x) == dict and 'id' in x:
                x['id'] = str(x['id'])
        for x in self.df['lieuExecution']:
            if type(x) == dict and 'code' in x:
                x['code'] = str(x['code'])
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
        self.df.loc[bool_nan_type, "_type"] = self.df.loc[bool_nan_type, "typeContrat"].map(dict_mapping)
        cols_to_drop = ["typeContrat", "lieuExecution", "ReferenceAccordCadre"] # On supprime donc typeContrat et lieuExecution est maintenant vide 
        # ReferenceAccordCadre n'a que 6 valeurs non nuls sur 650k lignes et en plus cette colonne n'existe pas dans v1. Je supprime.
        self.df = self.df.drop(cols_to_drop, axis=1)
        #Si il y a des Nan dans modifications, on met une liste vide pour coller au format du v1
        mask_modifications_nan = self.df.loc[:, "modifications"].isnull()
        self.df.modifications.loc[mask_modifications_nan] = self.df.modifications.loc[mask_modifications_nan].apply(lambda x: [])

    def drop_duplicate(self):
        """L'Étape drop duplicate supprime les duplicats purs après avoir supprimé les espaces et
        convertis l'ensemble du DataFrame en string."""
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
        logging.info(f"Nombre de marchés dans Df après suppression des doublons : {len(self.df)}")
        with open('pickle_test.pkl', "wb") as f:
            pickle.dump(self.dataframes, f)

    def export(self):
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
        with open('pickleselfdf.pkl', 'wb') as f:
            pickle.dump(self.df, f)
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
        with open("results/decpv2.json", 'w') as f:
            json.dump(dico, f, indent=2, ensure_ascii=False)
        json_size = os.path.getsize(r'results/decp.json')
        logging.info("Exportation JSON OK")
        logging.info(f"Taille de decp.json : {json_size}")
