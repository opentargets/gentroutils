"""Validate gwas catalog manual curation file."""

from __future__ import annotations

import logging
import re
import sys
from enum import Enum
from pathlib import Path
from typing import TypeVar

import click
import great_expectations as gx
from click import Argument, BadParameter
from great_expectations import expectations as gxe

T = TypeVar("T")


logger = logging.getLogger("gentroutils")
DATASOURCE_NAME = "GWAS Catalog curation"


class Lnum(Enum):
    """List convertable enum."""

    @classmethod
    def as_list(cls) -> list[T]:
        """Convert enum to list of strings."""
        return list(map(lambda c: c.value, cls))


class ColumnSet(Lnum):
    """Expected column names for curation file."""

    STUDY_ID = "studyId"
    STUDY_TYPE = "studyType"
    FLAG = "analysisFlag"
    QUALITY_CONTROL = "qualityControl"
    IS_CURATED = "isCurated"
    PUBMED = "pubmedId"
    PUBLICATION_TITLE = "publicationTitle"
    TRAIT = "traitFromSource"


class StudyType(Lnum):
    """Expected studyType column values."""

    NO_LICENCE = "no_licence"
    PQTL = "pQTL"


class AnalysisFlag(Lnum):
    """Expected analysisFlag column values."""

    CC = "Case-case study"
    EXWAS = "ExWAS"
    GXE = "GxE"
    GXG = "GxG"
    METABOLITE = "Metabolite"
    MULTIVARIATE = "Multivariate analysis"
    NON_ADDITIVE = "Non-additive model"


class IsCurated(Lnum):
    """Expected isCurated column values."""

    YES = True


def _validate_input_file_name(_: click.Context, param: Argument, value: str) -> str:
    """Assert file comes from local fs and exists."""
    logger.debug("Validating %s variable with %s value", param, value)
    import os

    logger.info(os.getcwd())
    pattern = re.compile(r"^[\w*/.-]*$")
    _match = pattern.fullmatch(value)
    if not _match:
        logger.error("%s is not a local file.", value)
        raise BadParameter("Provided path is not local.")
    p = Path(value)
    if p.is_dir():
        logger.error("%s is a directory.", value)
        raise BadParameter("Provided path is a directory.")
    if not p.exists():
        logger.error("%s does not exit.", value)
        raise BadParameter("Provided path does not exist.")
    return str(p)


def split_source_path(source_path: str) -> tuple[Path, str]:
    """Split the source path into directory name and filename"""
    p = Path(source_path)
    return p.parent, p.name


@click.command(name="validate-gwas-curation")
@click.argument("source_path", type=click.UNPROCESSED, callback=_validate_input_file_name)
@click.pass_context
def validate_gwas_curation(ctx: click.Context, source_path: str) -> None:  # noqa: DOC101, D0C103
    """Validate GWAS catalog manual curation file.

    \b
    gentroutils -vvv validate-gwas-curation GWAS_Catalog_study_curation.tsv

    """
    logger.info("Using %s as curation input.", source_path)

    dry_run = ctx.obj["dry_run"]
    if dry_run:
        logger.info("Running in --dry-run mode, exitting.")
        sys.exit(0)

    logger.info("Building great expectations context...")
    context = gx.get_context(mode="ephemeral")
    directory, file = split_source_path(source_path)
    data_source = context.data_sources.add_pandas_filesystem(name=DATASOURCE_NAME, base_directory=directory)

    logger.info("Using %s datasource.", DATASOURCE_NAME)
    logger.debug("Adding csv asset from %s", file)
    file_tsv_asset = data_source.add_csv_asset(name="manual_curation", sep="\t", header=0)
    logger.debug("Adding batch definion path %s", file)
    batch_definition = file_tsv_asset.add_batch_definition_path(name="manual_curation", path=file)

    logger.info("Building expectation suite...")

    suite = gx.ExpectationSuite(name="Curation Validation")
    context.suites.add(suite)
    suite.add_expectation(gxe.ExpectTableColumnsToMatchSet(column_set=ColumnSet.as_list(), exact_match=True))
    suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=ColumnSet.PUBMED.value, type_="int"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=ColumnSet.PUBLICATION_TITLE.value, type_="str"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=ColumnSet.TRAIT.value, type_="str"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=ColumnSet.STUDY_ID.value, type_="str"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=ColumnSet.STUDY_TYPE.value, type_="str"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=ColumnSet.FLAG.value, type_="str"))
    suite.add_expectation(
        gxe.ExpectColumnDistinctValuesToBeInSet(column=ColumnSet.STUDY_TYPE.value, value_set=StudyType.as_list())
    )
    suite.add_expectation(gxe.ExpectColumnDistinctValuesToBeInSet(column=ColumnSet.FLAG.value, value_set=AnalysisFlag.as_list()))
    suite.add_expectation(gxe.ExpectColumnValueLengthsToEqual(column=ColumnSet.PUBMED.value, value=8))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(column=ColumnSet.STUDY_ID.value, regex=r"^GCST\d+$"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=ColumnSet.STUDY_ID.value))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column=ColumnSet.STUDY_ID.value))
    suite.save()
    logger.info("Building validation definition...")
    validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite, name="Curation Validation")
    result = validation_definition.run()

    logger.info(
        click.style("Validation succeded" if result["success"] else "Validation failed", "green" if result["success"] else "red")
    )
    if not result["success"]:
        for res in result["results"]:
            if not res["success"]:
                logger.error(
                    "Expectation %s for column %s run with %s ",
                    res["expectation_config"]["type"],
                    res["expectation_config"]["kwargs"]["column"]
                    if "column" in res["expectation_config"]["kwargs"]
                    else res["expectation_config"]["kwargs"]["column_set"],
                    "succeded" if res["success"] else "failed",
                )
                logger.error(res)
        sys.exit(1)
