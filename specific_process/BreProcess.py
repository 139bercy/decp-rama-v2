from general_process.source_process import SourceProcess


class BreProcess(SourceProcess):
    def __init__(self):
        super().__init__("bre")

    def _url_init(self):
        super()._url_init()

    def get(self):
        super().get()

    def convert(self):
        super().convert()

    def fix(self):
        super().fix()
