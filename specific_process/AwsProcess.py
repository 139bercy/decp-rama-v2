from general_process.SourceProcess import SourceProcess


class AwsProcess(SourceProcess):
    def __init__(self):
        super().__init__("aws")

    def _url_init(self):
        super()._url_init()

    def fix(self):
        super().fix()
        # self.df['dureeMois'] = self.df['dureeMois'].astype(str)
        # self.df['montant'] = self.df['montant'].astype(str)
        # On se ramène au format souhaité pour titulaires, modifications et concessionnaires
        self.df['modifications'] = [x if str(x) == 'nan' or str(x) == '[]'
                                    else ([{'modification': [y for y in x]}] if len(x) > 1
                                          else [{'modification': x[0]}])
                                    for x in self.df['modifications']]
        self.df['titulaires'] = [x if str(x) == 'nan' or str(x) == '[]'
                                 else ([{'titulaire': [y for y in x]}] if len(x) > 1 else [{'titulaire': x[0]}])
                                 for x in self.df['titulaires']]
        self.df['concessionnaires'] = [x if str(x) == 'nan' or str(x) == '[]'
                                       else ([{'concessionnaire': [y for y in x]}]
                                             if len(x) > 1 else [{'concessionnaire': x[0]}])
                                       for x in self.df['concessionnaires']]

        # cette étape permet de gérer les différences de formats entre XML et Json convertis en dataframe
        for x in self.df['modifications']:
            if len(x) > 0:
                y = x[0]['modification']
                if type(y) is list:
                    for i in range(len(y)):
                        if 'titulaires' in x[0]['modification'][i]:
                            z = x[0]['modification'][i]['titulaires']
                            x[0]['modification'][i]['titulaires'] = \
                                ([y for y in z]) if len(z) > 1 else [z[0]]
                else:
                    if 'titulaires' in x[0]['modification']:
                        z = x[0]['modification']['titulaires']
                        # Dans les données si il y a un champ titulaire vide, ça crash. Corrigeons ça
                        if z != []:
                            x[0]['modification']['titulaires'] = \
                                ([y for y in z]) if len(z) > 1 else [z[0]]
                        # Sinon on fait rien car le champ titulaire est vide.
