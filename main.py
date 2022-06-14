from general_process.process_factory import ProcessFactory
from general_process.global_process import GlobalProcess


def main():
    if __name__ == "__main__":
        p = ProcessFactory()
        p.getProcess()
        gp = GlobalProcess()
        gp.dataframes = p.dataframes
        gp.merge_all()
        gp.drop_duplicate()
        gp.export_to_xml()
        gp.export_to_json()


main()
