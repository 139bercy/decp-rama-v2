from general_process.source_process import SourceProcess
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd


class BfcProcess(SourceProcess):
    def __init__(self):
        super().__init__(6)

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        pass
