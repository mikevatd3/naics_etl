"""
This script loads the NAICS tables to the database -- it also is an 
example standard transformation and load script.

Key features:
    - Using a pandera schema to validate the data and to capture metadata.
    - Using logging to track when the file was loaded.

Define Each Field that you'd like to include (Something else will have to 
be done for ACS loads).
"""

import click
import pandas as pd
import pandera as pa
from pandera.typing import Series
from pandera.errors import SchemaError, SchemaErrors
from sqlalchemy import text
import tomli

from naics import setup_logging, db_engine, metadata_engine
from metadata_audit.capture import record_metadata
from sqlalchemy.orm import sessionmaker


# Set up config and logging for script -- Boilerplate for every script.
logger = setup_logging()

table_name = "industry_detail"

with open("metadata.toml", "rb") as md:
    metadata = tomli.load(md)


class NAICSIndustryDetail(pa.DataFrameModel):
    """
    This is one of two tables that make sense to include
    """

    id: int = pa.Field(unique=True, nullable=False)
    code: str = pa.Field() # Not required to be unique in this table
    description: str = pa.Field(nullable=True)

    class Config:  # type: ignore
        strict = True
        coerce = True

    @pa.check("code")
    def code_len(cls, code: Series[str]) -> bool:
        """
        The codes should all be the same length -- some are all "****"
        to reflect designations that have changed.
        """
        return (code.str.len() == 6).all()

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
    
    file = pd.read_csv(edition["raw_path"])


    result = (
        pd.read_csv(edition["raw_path"])
        .rename(
            columns={
                "Unnamed: 0": "id",
                "NAICS22": "code",
                "INDEX ITEM DESCRIPTION": "description",
            }
        )
    )

    logger.info(f"Cleaning {table_name} was successful validating schema.")


    try:
        validated = NAICSIndustryDetail.validate(result)
        logger.info(
            f"Validating {table_name} was successful. Recording metadata."
        )
    except (SchemaError, SchemaErrors) as e:
        logger.error(f"Validating {table_name} failed.", e)

    return 

    with metadata_engine.connect() as db:
        logger.info("Connected to metadata schema.")

        record_metadata(
            NAICSIndustryDetail,
            __file__,
            table_name,
            metadata,
            edition_date,
            result,
            sessionmaker(bind=db)(),
            logger,
        )

        db.commit()
        logger.info("successfully recorded metadata")

    with db_engine.connect() as db:
        logger.info("Metadata recorded, pushing data to db.")

        validated.to_sql(  # type: ignore
            table_name, db, index=False, schema="naics", if_exists="replace"
        )


if __name__ == "__main__":
    main()
