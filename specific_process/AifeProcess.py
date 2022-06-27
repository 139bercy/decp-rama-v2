from general_process.SourceProcess import SourceProcess
import json

class AifeProcess(SourceProcess):
    def __init__(self):
        super().__init__("aife")

    def _url_init(self):
        super()._url_init()

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        self.df.drop([index for index, rows in self.df.iterrows() if type(rows['acheteur']) != dict], inplace=True)
        super().fix()
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: x if x is None or type(x) == list else [x])
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if x is None else json.loads(json.dumps(x)))
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if type(x) == list else [] if x is None else [x])

