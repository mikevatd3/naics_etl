"""
This script loads the NAICS tables to the database -- it also is an 
example standard transformation and load script.

Key features:
    - Using a pandera schema to validate the data and to capture metadata.
    - Using logging to track when the file was loaded.

Define Each Field that you'd like to include (Something else will have to 
be done for ACS loads).
"""

import logging

import click
import pandas as pd
import pandera as pa
from pandera.typing import Series
from pandera.errors import SchemaError, SchemaErrors
import tomli

from naics import setup_logging, db_engine, metadata_engine
from metadata_audit.capture import record_metadata
from sqlalchemy.orm import Session

# Set up config and logging for script -- Boilerplate for every script.
with open("config.toml", "rb") as f:
    config = tomli.load(f)

logger = logging.getLogger(config["app"]["name"])
setup_logging()


table_name = "naics_descriptions"

with open("metadata.toml", "rb") as md:
    metadata = tomli.load(md)


# Every loader script starts with a pydantic model -- This is both to 
# validate the clean-up process output and to ensure that fields agree
# with the metadata provided to the metadata system.
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

    logger.info(f"Cleaning {table_name} was successful validating schema.")

    # Validate
    try:
        validated = NAICSDescriptions.validate(result)
        logger.info(
            f"Validating {table_name} was successful. Recording metadata."
        )
    except SchemaError | SchemaErrors as e:
        logger.error(f"Validating {table_name} failed.", e)
    

    with metadata_engine.connect() as db:
        logger.info("Connected to metadata schema.")
        record_metadata(
            NAICSDescriptions,
            __file__,
            table_name,
            metadata,
            edition_date,
            result,
            Session(db),
            logger
        )

    with db_engine.connect() as db:

        logger.info("Metadata recorded, pushing data to db.")

        validated.to_sql(  # type: ignore
            "naics_codes", db, index=False, schema="naics", if_exists="append"
        )


if __name__ == "__main__":
    main()
