from specific_process.PesProcess import PesProcess
from specific_process.AwsProcess import AwsProcess
from specific_process.LyonProcess import LyonProcess
from specific_process.BfcProcess import BfcProcess
# Problème d'importation des url a fixer # A fixer, car les fichiers évoluent mensuellement.
from specific_process.AifeProcess import AifeProcess
from specific_process.EmarProcess import EmarProcess
from specific_process.MegaProcess import MegaProcess
from specific_process.DecpAwsProcess import DecpAwsProcess
# from specific_process.MaxiProcess import MaxiProcess
# Le get ne marche pas plus on n'a pas accès au github # A regaredr
from specific_process.BreProcess import BreProcess
# Source non traitée pour l'instant
import logging


class ProcessFactory:

    def __init__(self, process=None):
        """Création de la liste des Processus qui correspondent chacun à une classe importée en début de document."""
        #self.processes = [PesProcess, AwsProcess, AifeProcess, EmarProcess, LyonProcess, MegaProcess]
        self.processes = [DecpAwsProcess, BfcProcess, PesProcess, AwsProcess, AifeProcess, EmarProcess, LyonProcess, MegaProcess]
        #self.processes = [PesProcess, LyonProcess]
        self.dataframes = []
        # si on lance main avec un process spécifié :
        if process:
            for proc in self.processes:
                if proc.__name__ == process:
                    self.process = proc
                    break

    def run_processes(self):
        """Création d'une boucle (1 source=1 itération) qui appelle chacun des processus de chaque source."""
        for process in self.processes:
            logging.info(f"------------------------------{process.__name__}------------------------------")
            p = process()
            #p.get()
            p.convert()
            p.fix()
            self.dataframes.append(p.df)

    def run_process(self):
        """Lance un seul process pour tester"""
        logging.info(f"------------------------------{self.process.__name__}------------------------------")
        p = self.process()
        p.get()
        p.convert()
        p.fix()
        self.dataframes.append(p.df)
