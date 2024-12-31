"""
This script loads the NAICS tables to the database -- it also is an 
example standard transformation and load script.

Key features:
    - Using a pandera schema to validate the data and to capture metadata.
    - Using logging to track when the file was loaded.


Define Each Field that you'd like to include (Something else will have to 
be done for ACS loads).
"""

from pathlib import Path
import logging

import click
import pandas as pd
import pandera as pa
from pandera.typing import Series
import tomli

from naics import setup_logging, db_engine
from metadata_audit.capture import record_metadata


class NAICSDescriptions(pa.DataFrameModel):
    """
    This is one of two tables that make sense to include
    """

    code: str = pa.Field(unique=True)
    title: str = pa.Field()
    description: str = pa.Field(nullable=True)

    class Config:  # type: ignore
        strict = True
        coerce = True

    @pa.check("description")
    def max_nulls(cls, description: Series[str]) -> bool:
        """
        It's okay for some of these to be null, but if there are too many
        it could indicate a problem.
        """
        return description.isna().sum() < 200


@click.command()
@click.argument("edition_date")
def main(edition_date):
    table_name = "naics_descriptions"

    with open("config.toml", "rb") as f:
        config = tomli.load(f)

    logger = logging.getLogger(config["app"]["name"])

    # 'Setup logging' makes sure the logging is saved in the right place.
    setup_logging()

    with open("metadata.toml", "rb") as md:
        metadata = tomli.load(md)
    
    edition = metadata["tables"][table_name]["editions"][edition_date]

    result = (
        pd.read_csv(edition["raw_path"])
        .rename(
            columns={
                "Code": "code",
                "Title": "title",
                "Description": "description",
            }
        )
        .drop("Unnamed: 0", axis=1)
    )

    # Validate
    validated = NAICSDescriptions.validate(result)

    record_metadata(
        NAICSDescriptions,
        __file__,
        table_name, 
        metadata, 
        edition_date, 
        result
    )

#     with db_engine.connect() as db:
#         validated.to_sql(
#             "naics_codes", db, index=False, schema="naics", if_exists="replace"
#         )


if __name__ == "__main__":
    main()
