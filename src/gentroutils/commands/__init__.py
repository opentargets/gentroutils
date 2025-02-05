"""CLI submodules for gentroutils package."""

from gentroutils.commands.update_gwas_curation_metadata import (
    update_gwas_curation_metadata_command,
)
from gentroutils.commands.validate_gwas_curation import validate_gwas_curation

__all__ = [
    "update_gwas_curation_metadata_command",
    "validate_gwas_curation",
]
