from general_process.source_process import SourceProcess


class AifeProcess(SourceProcess):
    def __init__(self):
        self.id = 2
        super().__init__()

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()

