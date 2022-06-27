from general_process.SourceProcess import SourceProcess


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
        self.df.drop([index for index, rows in df.iterrows() if type(rows['acheteur']) != dict], inplace=True)
        super().fix()

