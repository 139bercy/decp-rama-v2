import pandas as pd
from dict2xml import dict2xml
import json


class GlobalProcess:
    def merge_all(self):
        self.df = pd.concat(self.dataframes, ignore_index=True)

    def drop_duplicate(self):
        pass

    def export_to_xml(self):
        dico = {'marches': [{'marche': {k: v for k, v in m.items() if str(v) != 'nan'}}
                            for m in self.df.to_dict(orient='records')]}
        with open("results/decp.xml", 'w') as f:
            f.write(dict2xml(dico))

    def export_to_json(self):
        dico = {'marches': [{k: v for k, v in m.items() if str(v) != 'nan'}
                            for m in self.df.to_dict(orient='records')]}
        with open("results/decp.json", 'w') as f:
            json.dump(dico, f, indent=2, ensure_ascii=False)
