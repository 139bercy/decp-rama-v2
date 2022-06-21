from general_process.ProcessFactory import ProcessFactory
from general_process.GlobalProcess import GlobalProcess
import logging


def main():
    p = ProcessFactory()
    p.run_processes()
    logging.info(f"------------------------------GLOBAL-PROCESS------------------------------")
    gp = GlobalProcess()
    gp.dataframes = p.dataframes
    logging.info("  ÉTAPE MERGE ALL")
    gp.merge_all()
    logging.info("  ÉTAPE FIX ALL")
    gp.fix_all()
    gp.drop_duplicate()
    logging.info("  ÉTAPE EXPORTATION")
    gp.export_to_xml()
    gp.export_to_json()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    logging.root.setLevel(logging.INFO)

    main()
