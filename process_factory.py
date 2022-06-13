from sources_class import ProcessPesMarches, ProcessAWS


class ProcessFactory:

    def __init__(self):
        self.processes = [ProcessPesMarches, ProcessAWS]
        self.li_df = []

    def getProcess(self):
        for process in self.processes:
            p = process()
            # p.get()
            p.convert()
            p.fix()
            self.li_df.append(p.df)
