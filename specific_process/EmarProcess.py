from general_process.source_process import SourceProcess


class EmarProcess(SourceProcess):
    def __init__(self):
        self.id = 3
        super().__init__()

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()
