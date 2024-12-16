from pathlib import Path
import logging
import pandas as pd
import geopandas as gpd

from loader import build_workflow, LoadFileType, StopThePresses
from naics import config, db_engine, setup_logging


logger = logging.getLogger(config["app"]["name"])
setup_logging()


def cleanup_function(input_file: pd.DataFrame | gpd.GeoDataFrame):
    logger.info(input_file.columns)

    raise StopThePresses("No cleanup function defined.")


def main():
    workflow = build_workflow(
        config,
        "naics",
        Path.cwd(),
        cleanup_function,
        db_engine,
        LoadFileType.CSV,
    )

    workflow()


if __name__ == "__main__":
    main()
