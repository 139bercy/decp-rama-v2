from source_process import SourceProcess


class AWSProcess(SourceProcess):
    def __init__(self):
        self.id = 1
        super().__init__()

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()
        # self.df['dureeMois'] = self.df['dureeMois'].astype(str)
        # self.df['montant'] = self.df['montant'].astype(str)
        # On se ramène au format souhaité pour titulaires, modifications et concessionnaires
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: [{'titulaire': y} for y in x] if str(x) != 'nan' else x)
        self.df['modifications'] = self.df['modifications'].apply(
            lambda x: [{'modification': y} for y in x] if str(x) != 'nan' else x)
        self.df['concessionnaires'] = self.df['concessionnaires'].apply(
            lambda x: [{'concessionnaire': y} for y in x] if str(x) != 'nan' else x)
