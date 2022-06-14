import sys

import pandas as pd

sys.path.append("specific_process")
from PesProcess import PesProcess
from AwsProcess import AwsProcess


class ProcessFactory:

    def __init__(self):
        self.processes = [PesProcess, AwsProcess]
        self.dataframes = pd.DataFrame()

    def getProcess(self):
        for process in self.processes:
            p = process()
            p.get()
            p.convert()
            p.fix()
            self.dataframes.append(p.df)
