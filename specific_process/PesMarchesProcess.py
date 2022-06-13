from source_process import SourceProcess
import json


class PesMarchesProcess(SourceProcess):
    def __init__(self):
        self.url = ["https://files.data.gouv.fr/decp/dgfip-pes-decp.xml"]
        self.file_name = ["data.gouv.fr_pes"]
        self.source = "data.gouv.fr_pes"
        self.format = "xml"
        self.df = []

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()
        # On enlève les OrderedDict et on se ramène au format souhaité
        self.df['acheteur'] = self.df['acheteur'].apply(lambda x: json.loads(json.dumps(x)))
        self.df['lieuExecution'] = self.df['lieuExecution'].apply(
            lambda x: json.loads(json.dumps(x)))
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: json.loads(json.dumps(x)))
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: x if x is None or type(x) == list else [x])
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if x is None else json.loads(json.dumps(x)))
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if type(x) == list else [] if x is None else [x])
        # Supression des doublons
        # self.df = self.df_drop_duplicates(self.df)
