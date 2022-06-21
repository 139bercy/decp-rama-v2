from general_process.ProcessFactory import ProcessFactory
from general_process.GlobalProcess import GlobalProcess
import logging


def main():
    p = ProcessFactory()
    p.run_processes()
    gp = GlobalProcess()
    gp.dataframes = p.dataframes
    gp.merge_all()
    gp.fix_all()
    gp.drop_duplicate()
    gp.export()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    logging.root.setLevel(logging.INFO)

    main()
