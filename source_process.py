import wget
import os
import json
import pandas as pd
import xmltodict


class SourceProcess:
    def get(self):
        os.makedirs(f"sources/{self.source}", exist_ok=True)
        for i in range(len(self.url)):
            wget.download(self.url[i], f"sources/{self.source}/{self.file_name[i]}.{self.format}")

    def convert(self):
        if self.format == 'xml':
            li = []
            for i in range(len(self.url)):
                with open(
                        f"sources/{self.source}/{self.file_name[i]}.{self.format}") as xml_file:
                    dico = xmltodict.parse(xml_file.read())
                    df = pd.DataFrame.from_dict(dico['marches']['marche'])
                li.append(df)
            df = pd.concat(li)
            self.df = df
        elif self.format == 'json':
            li = []
            for i in range(len(self.url)):
                with open(
                        f"sources/{self.source}/{self.file_name[i]}.{self.format}") as json_file:
                    dico = json.load(json_file)
                li.append(pd.DataFrame.from_dict(dico['marches']))
            df = pd.concat(li)
            self.df = df

    def fix(self):
        self.df = self.df.assign(source=self.source)
