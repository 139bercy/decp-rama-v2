from general_process.SourceProcess import SourceProcess
import json


class PesProcess(SourceProcess):
    def __init__(self):
        super().__init__("pes")

    def fix(self):
        super().fix()
        # On enlève les None des "titulaires" et "modifications"
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: x if type(x) == list else [] if x is None or str(x) == 'nan' else [x])
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if x is None or str(x) == 'nan' else json.loads(json.dumps(x)))
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if type(x) == list else [] if x is None or str(x) == 'nan' else [x])