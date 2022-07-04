from general_process.ProcessFactory import ProcessFactory
from general_process.GlobalProcess import GlobalProcess
import logging


def main():
    """La fonction main() appelle tour à tour les processus spécifiques (ProcessFactory.py/SourceProcess.py) et les
    étapes du Global Process (GlobalProcess.py)."""
    p = ProcessFactory()
    p.run_processes()
    gp = GlobalProcess()
    gp.dataframes = p.dataframes
    gp.merge_all()
    gp.fix_all()
    gp.drop_duplicate()
    gp.export()


if __name__ == "__main__":
    """Lorsqu'on appelle la fonction main (courante), on définit le niveau de logging et le format d'affichage."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    logging.root.setLevel(logging.INFO)

    main()
