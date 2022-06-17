from general_process.process_factory import ProcessFactory
from general_process.global_process import GlobalProcess
from general_process.init import Init
import logging


def main():
    if __name__ == "__main__":
        i = Init()
        i.get_init()
        i.get_urls()
        p = ProcessFactory()
        gp = GlobalProcess()
        p.getprocess()
        gp.dataframes = p.dataframes
        gp.merge_all()
        gp.fix_all()
        gp.drop_duplicate()
        gp.export_to_xml()
        gp.export_to_json()


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logging.root.setLevel(logging.INFO)

main()
