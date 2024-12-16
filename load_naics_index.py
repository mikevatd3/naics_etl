from pathlib import Path
import logging
import pandas as pd
import geopandas as gpd

from loader import build_workflow, LoadFileType, StopThePresses
from naics import config, db_engine, setup_logging


logger = logging.getLogger(config["app"]["name"])
setup_logging()


def cleanup_function(input_file: pd.DataFrame | gpd.GeoDataFrame):

    result = input_file.rename(columns={
        'Unnamed: 0': "id", 
        'NAICS22': "naics_2022", 
        'INDEX ITEM DESCRIPTION': "description"
    })
    
    logger.info(result.head())
    
    return result


def main():
    workflow = build_workflow(
        config,
        "naics_detail",
        Path.cwd(),
        cleanup_function,
        db_engine,
        LoadFileType.CSV,
    )

    workflow()


if __name__ == "__main__":
    main()
