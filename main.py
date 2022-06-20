from general_process.process_factory import ProcessFactory
from general_process.global_process import GlobalProcess
import logging


def main():
    p = ProcessFactory()
    p.run_processes()
    # gp = GlobalProcess()
    # gp.dataframes = p.dataframes
    # gp.merge_all()
    # gp.fix_all()
    # gp.drop_duplicate()
    # gp.export_to_xml()
    # gp.export_to_json()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    logging.root.setLevel(logging.INFO)

    main()
