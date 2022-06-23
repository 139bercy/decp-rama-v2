from general_process.SourceProcess import SourceProcess


class MegaProcess(SourceProcess):
    def __init__(self):
        super().__init__("mega")

    def _url_init(self):
        super()._url_init()

    def get(self):
        super().get()

    def convert(self):
        # les fichiers xml de megalis ne sont pas au bon format
        for i in range(len(self.url)):
            with open(f"sources/{self.source}/{self.file_name[i]}.{self.format}", 'r') as file:
                data = file.read().splitlines(True)
            with open(f"sources/{self.source}/{self.file_name[i]}.{self.format}", 'w') as file:
                file.write('<?xml version= "1.0"  encoding= "utf8" ?>\n')
                file.writelines(data[1:])
        super().convert()

    def fix(self):
        super().fix()
        self.df['titulaires'] = self.df['titulaires'].apply(
            lambda x: x if x is None or type(x) == list else [x])

