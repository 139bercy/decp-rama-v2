from specific_process.PesProcess import PesProcess
from specific_process.AwsProcess import AwsProcess
from specific_process.LyonProcess import LyonProcess
from specific_process.BfcProcess import BfcProcess
from specific_process.AifeProcess import AifeProcess
from specific_process.EmarProcess import EmarProcess
from specific_process.MegaProcess import MegaProcess


class ProcessFactory:

    def __init__(self):
        self.processes = [PesProcess, AwsProcess, LyonProcess, BfcProcess, AifeProcess, EmarProcess, MegaProcess]
        self.dataframes = []

    def run_processes(self):
        for process in self.processes:
            p = process()
            p.get()
            # p.convert()
            # p.fix()
            # self.dataframes.append(p.df)
