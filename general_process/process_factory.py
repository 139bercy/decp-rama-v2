import sys
sys.path.append("specific_process")
from PesMarchesProcess import PesMarchesProcess
from AWSProcess import AWSProcess


class ProcessFactory:

    def __init__(self):
        self.processes = [PesMarchesProcess, AWSProcess]
        self.dataframes = []

    def getProcess(self):
        for process in self.processes:
            p = process()
            p.get()
            p.convert()
            p.fix()
            self.dataframes.append(p.df)
