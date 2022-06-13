from process_factory import ProcessFactory
from global_process import GlobalProcess

p = ProcessFactory()
p.getProcess()

gp = GlobalProcess()
gp.li_df = p.li_df
gp.merge_all()
gp.drop_duplicate()
gp.export_to_xml()
gp.export_to_json()
