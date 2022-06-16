from general_process.source_process import SourceProcess
import json

class LyonProcess(SourceProcess):
    def __init__(self):
        super().__init__(4)

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
        # On remplace datePublicationDonnees par la première (la plus récente)
        self.df['datePublicationDonnees'] = self.df['datePublicationDonnees'].apply(
            lambda x: x[0])

