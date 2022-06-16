from specific_process.PesProcess import PesProcess
from specific_process.AwsProcess import AwsProcess
from specific_process.LyonProcess import LyonProcess


class ProcessFactory:

    def __init__(self):
        self.processes = [PesProcess, AwsProcess, LyonProcess]
        self.dataframes = []

    def getprocess(self):
        for process in self.processes:
            p = process()
            p.get()
            p.convert()
            p.fix()
            self.dataframes.append(p.df)
