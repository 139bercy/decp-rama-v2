from general_process.SourceProcess import SourceProcess


class EmarProcess(SourceProcess):
    def __init__(self):
        super().__init__("emar")

    def _url_init(self):
        super()._url_init()

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()
        self.df['modifications'] = [x if str(x) == 'nan' or str(x) == '[]'
                                    else ([{'modification': [y for y in x]}] if len(x) > 1
                                          else [{'modification': x[0]}])
                                    for x in self.df['modifications']]
        self.df['titulaires'] = [x if str(x) == 'nan' or str(x) == '[]'
                                 else ([{'titulaire': [y for y in x]}] if len(x) > 1 else [{'titulaire': x[0]}])
                                 for x in self.df['titulaires']]

        for x in self.df['modifications']:
            if len(x) > 0:
                y = x[0]['modification']
                if type(y) is list:
                    for i in range(len(y)):
                        if 'titulaires' in x[0]['modification'][i]:
                            z = x[0]['modification'][i]['titulaires']
                            x[0]['modification'][i]['titulaires'] = \
                                (dict({'titulaire': [y for y in z]}) if len(z) > 1 else {'titulaire': z[0]})
                else:
                    if 'titulaires' in x[0]['modification']:
                        z = x[0]['modification']['titulaires']
                        x[0]['modification']['titulaires'] = \
                            (dict({'titulaire': [y for y in z]}) if len(z) > 1 else {'titulaire': z[0]})
