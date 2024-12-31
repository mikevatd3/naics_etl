from pathlib import Path
import logging
import pandas as pd

from naics import config, setup_logging

logger = logging.getLogger(config["app"]["name"])
setup_logging()


def main():
    logging.info("Changing over xlsx from the census site to csv")
    file = pd.read_excel(
        Path.cwd().parent.parent
        / "data"
        / "naics"
        / "raw"
        / "2022_NAICS_Index_File.xlsx"
    )
    file.to_csv(
        Path.cwd().parent.parent
        / "data"
        / "naics"
        / "raw"
        / "naics_index_file_2022.csv"
    )

    file = pd.read_excel(
        Path.cwd().parent.parent
        / "data"
        / "naics"
        / "raw"
        / "2022_NAICS_Descriptions.xlsx"
    )
    file.to_csv(
        Path.cwd().parent.parent
        / "data"
        / "naics"
        / "raw"
        / "naics_descriptions_2022.csv"
    )


if __name__ == "__main__":
    main()
