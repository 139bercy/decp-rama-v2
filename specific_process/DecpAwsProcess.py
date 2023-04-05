from general_process.SourceProcess import SourceProcess
import os
import json
import wget
import pandas as pd
import ast

class DecpAwsProcess(SourceProcess):
    def __init__(self):
        with open("metadata/metadata.json", 'r+') as f:
            self.metadata = json.load(f)
        self.key = "decp_aws"
        self.source = self.metadata[self.key]["code"]
        self.format = self.metadata[self.key]["format"]
        self.url_source = self.metadata[self.key]["url_source"]
        self.file_name = ["decp_aws.json"]
        self.df = pd.DataFrame()
        self.local_path = os.path.join("sources", self.source, self.file_name[0])

    def _url_init(self):
        pass

    def get(self):
        """ Un peu particulier, on pointe sur data eco"""
        os.makedirs(f"sources/{self.source}", exist_ok=True)
        
        wget.download(self.url_source, self.local_path)

    def convert(self):
        with open(self.local_path, "r") as f:
            awsjson = json.load(f)
        self.df = pd.json_normalize(awsjson)
        # Je ne sais pas pourquoi mais lorsque l'on télécharge depuis data eco les listes sont renvoyés comme des strings.
        # Reconvertissons les en liste.
        self.df.modifications = self.df.modifications.apply(lambda x: ast.literal_eval(x))
        self.df.titulaires = self.df.titulaires.apply(lambda x:ast.literal_eval(x))

        # Maintenant il faut que le format des titulaires soit au même format que les autres pour que le processing global se déroule bien.
        self.df.titulaires = self.df.titulaires.apply(lambda x:[{"titulaire": x[0]}])
        self.df.modifications = self.df.modifications.apply(lambda x:[{"modification": x[0]}] if len(x)>0 else x)
        
        # Les None sont également des string du coup. Castons les
        self.df = self.df.replace("None", None, regex=True)

        # On applique le même traitement que dans la classe parent, ie on retire les "'" pour des " ". 
        # Ca posait problèmes car les clefs des dictionnaires était en simple quote (') et pas (").
        # Alors je n'applique pas ces modifications pour lesquelles je en vois pas d'utilité partciulière actuellement
        #self.df = self.df.replace("\'", " ", regex=True)
        dict_mapping = {"codecpv":"codeCPV", "type":"_type", "acheteur_id":"acheteur.id", "acheteur_nom":"acheteur.nom",
        "datenotification": "dateNotification", "datepublicationdonnees": "datePublicationDonnees",
        "lieuexecution_code": "lieuExecution.code", "lieuexecution_nom":"lieuExecution.nom", "lieuexecution_typecode": "lieuExecution.typeCode",
        "formeprix": "formePrix", "dureemois":"dureeMois", "datetransmissiondonneesetalab":"dateTransmissionDonneesEtalab"}
        self.df = self.df.rename(columns=dict_mapping)
    
    def fix(self):
        super().fix()

