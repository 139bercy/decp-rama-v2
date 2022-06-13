from process_factory import ProcessFactory
from global_process import GlobalProcess

if __name__ == "__main__":
    p = ProcessFactory()
    p.getProcess()
    gp = GlobalProcess()
    gp.dataframes = p.dataframes
    gp.merge_all()
    gp.drop_duplicate()
    gp.export_to_xml()
    gp.export_to_json()
