from source_process import SourceProcess


class AWSProcess(SourceProcess):
    def __init__(self):
        self.url = [
            "https://www.data.gouv.fr/fr/datasets/r/1db1b73c-5b7d-45fb-a823-bf637ce5ad03",
            "https://www.data.gouv.fr/fr/datasets/r/a03289f6-0460-4f6e-96e1-4f3e69360843",
            "https://www.data.gouv.fr/fr/datasets/r/2429e350-bf99-4f14-8c8c-c23611e268c8",
            "https://www.data.gouv.fr/fr/datasets/r/320eb2c3-1f93-4e7c-97bc-1276e1d61226",
            "https://www.data.gouv.fr/fr/datasets/r/e46944f5-127b-4071-907a-24c1ce1202da"]
        self.file_name = [
            f"marches-publics.info_{2018+i}" for i in range(5)]
        self.source = "marches-publics.info"
        self.format = "json"

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
        # Supression des doublons
        # df = df_drop_duplicates(df)
