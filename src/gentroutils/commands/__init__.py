"""CLI submodules for gentroutils package."""

from gentroutils.commands.prepare_curation_table import prepare_curation_table_command
from gentroutils.commands.update_gwas_curation_metadata import (
    update_gwas_curation_metadata_command,
)
from gentroutils.commands.validate_gwas_curation import validate_gwas_curation_command

__all__ = ["prepare_curation_table_command", "update_gwas_curation_metadata_command", "validate_gwas_curation_command"]
