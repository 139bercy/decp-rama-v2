from source_process import SourceProcess


class BreProcess(SourceProcess):
    def __init__(self):
        self.id = 8
        super().__init__()

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()
