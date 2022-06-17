import os
import logging
import json
from datetime import date
from dateutil.relativedelta import relativedelta


class Init:

    def __init__(self):
        with open('metadata/metadata_source.json', 'r+') as f:
            self.metadata = json.load(f)

    def get_init(self):
        logging.info("Supression des sources existantes")
        for root, dirs, files in os.walk("sources/", topdown=False):
            for name in files:
                if name != '.gitignore':
                    os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        logging.info("Supression des sources OK")

    def get_urls(self):
        logging.info("Récupération des urls sources à jour existants")
        # PES
        self.metadata[0]["url"] = [self.metadata[0]["url_source"]]
        # AWS
        lis_ext_url = ["1db1b73c-5b7d-45fb-a823-bf637ce5ad03", "a03289f6-0460-4f6e-96e1-4f3e69360843",
                       "2429e350-bf99-4f14-8c8c-c23611e268c8", "320eb2c3-1f93-4e7c-97bc-1276e1d61226",
                       "e46944f5-127b-4071-907a-24c1ce1202da"]
        self.metadata[1]["url"] = [f"{self.metadata[1]['url_source']}{lis_ext_url[x]}" for x in range(len(lis_ext_url))]
        # BFC
        delta = relativedelta(months=1)
        auj = date(date.today().year, date.today().month, 1)
        pre_date = date(2019, 1, 1)
        delta_total = relativedelta(auj, pre_date)
        nb_mois = delta_total.years * 12 + delta_total.months
        self.metadata[6]["url"] = [(date(pre_date.year, pre_date.month, pre_date.day) + delta * x).strftime(
            f"{self.metadata[6]['url_source']}marches-%Y-%m") for x in range(nb_mois)]
        logging.info("Récupération des urls OK")
        # LYON
        self.metadata[4]["url"] = [self.metadata[4]["url_source"]]
        # Export metadata.json
        with open('metadata/metadata.json', 'w') as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)