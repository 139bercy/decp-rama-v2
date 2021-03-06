from general_process.SourceProcess import SourceProcess
import json


class PesProcess(SourceProcess):
    def __init__(self):
        super().__init__("pes")

    def fix(self):
        super().fix()
        # On enlève les OrderedDict et on se ramène au format souhaité
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: x if x is None or type(x) == list else [x])
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if x is None else json.loads(json.dumps(x)))
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if type(x) == list else [] if x is None else [x])
