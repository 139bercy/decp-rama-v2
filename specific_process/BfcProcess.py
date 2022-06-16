from general_process.source_process import SourceProcess
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd


class BfcProcess(SourceProcess):
    def __init__(self):
        self.id_int = 6
        self.metadata = pd.read_json('metadata.json')
        self.url = self.metadata["url"][self.id_int]
        delta = relativedelta(months=1)
        auj = date(date.today().year, date.today().month, 1)
        pre_date = date(2019, 1, 1)
        delta_total = relativedelta(auj, pre_date)
        nb_mois = delta_total.years * 12 + delta_total.months
        self.url = [(date(pre_date.year, pre_date.month, pre_date.day) + delta * x).strftime(
            f"{self.url[0]}marches-%Y-%m") for x in range(nb_mois)]
        self.file_name = [f"{self.metadata['code'][self.id_int]}_{i}" for i in range(len(self.url))]
        self.source = self.metadata["code"][self.id_int]
        self.format = self.metadata["format"][self.id_int]
        self.df = pd.DataFrame()

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()
