from general_process.source_process import SourceProcess
import json
from datetime import date
from dateutil.relativedelta import relativedelta


class BfcProcess(SourceProcess):
    def __init__(self):
        super().__init__(6)

    def _url_init(self):
        delta = relativedelta(months=1)
        auj = date(date.today().year, date.today().month, 1)
        pre_date = date(2019, 1, 1)
        delta_total = relativedelta(auj, pre_date)
        nb_mois = delta_total.years * 12 + delta_total.months
        self.metadata["url"][self.id_int] = [(date(pre_date.year, pre_date.month, pre_date.day) + delta * x).strftime(
            f"{self.metadata['url_source'][self.id_int]}marches-%Y-%m") for x in range(nb_mois)]
        self.url = self.metadata["url"][self.id_int]
        self.file_name = [f"{self.metadata['code'][self.id_int]}_{i}" for i in range(len(self.url))]

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()
        self.df = self.df.drop(['contratTransverse'], axis=1)
        self.df = self.df.drop(['donneesComplementaires'], axis=1)
        # On enlève les OrderedDict et on se ramène au format souhaité
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: x if x is None or type(x) == list else [x])
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if x is None or str(x) == 'nan' else json.loads(json.dumps(x)))
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: x if type(x) == list else [] if x is None or str(x) == 'nan' else [x])
