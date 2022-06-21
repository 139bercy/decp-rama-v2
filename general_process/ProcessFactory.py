from specific_process.PesProcess import PesProcess
from specific_process.AwsProcess import AwsProcess
from specific_process.LyonProcess import LyonProcess
from specific_process.BfcProcess import BfcProcess
from specific_process.AifeProcess import AifeProcess
from specific_process.EmarProcess import EmarProcess
from specific_process.MegaProcess import MegaProcess
# from specific_process.MaxiProcess import MaxiProcess
# Le get ne marche pas plus on n'a pas accès au github
# from specific_process.BreProcess import BreProcess
# Source non traitée pour l'instant
import logging


class ProcessFactory:

    def __init__(self):
        self.processes = [PesProcess, AwsProcess, AifeProcess, EmarProcess, LyonProcess, BfcProcess, MegaProcess]
        self.dataframes = []

    def run_processes(self):
        for process in self.processes:
            logging.info(f"------------------------------{repr(process).split('.')[-2]}------------------------------")
            p = process()
            p.get()
            p.convert()
            p.fix()
            self.dataframes.append(p.df)
