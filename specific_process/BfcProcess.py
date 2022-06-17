from general_process.source_process import SourceProcess
import json


class BfcProcess(SourceProcess):
    def __init__(self):
        super().__init__(6)

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()
        self.df = self.df.drop(['contratTransverse'], axis=1)
        self.df = self.df.drop(['donneesComplementaires'], axis=1)
        # On enlève les OrderedDict et on se ramène au format souhaité
        self.df['acheteur'] = self.df['acheteur'].apply(lambda x: json.loads(json.dumps(x)))
        self.df['lieuExecution'] = self.df['lieuExecution'].apply(
            lambda x: json.loads(json.dumps(x)))
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: json.loads(json.dumps(x)))
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: x if x is None or type(x) == list else [x])
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if x is None or str(x) == 'nan' else json.loads(json.dumps(x)))
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if type(x) == list else [] if x is None or str(x) == 'nan' else [x])
