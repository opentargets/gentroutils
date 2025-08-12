"""Test tasks module."""

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gentroutils.tasks import GwasCatalogReleaseInfo, TemplateDestination


class TestTemplateDestination:
    """Test cases for TemplateDestination."""

    def test_format_destination(self):
        """Test formatting of destination with substitutions."""
        template_dest = TemplateDestination("gs://bucket/{release_date}/file.txt")
        formatted_dest = template_dest.format({"release_date": "2023-10-01"})
        assert formatted_dest.destination == "gs://bucket/2023-10-01/file.txt"
        assert formatted_dest.is_substituted is True

    def test_format_destination_no_substitution(self):
        """Test formatting without substitutions."""
        template_dest = TemplateDestination("gs://bucket/file.txt")
        formatted_dest = template_dest.format({})
        assert formatted_dest.destination == "gs://bucket/file.txt"
        assert formatted_dest.is_substituted is True


class TestGwasCatalogReleaseInfo:
    """Test cases for GwasCatalogReleaseInfo."""

    def test_release_info_initialization(self):
        """Test initialization of GwasCatalogReleaseInfo."""
        release_info = GwasCatalogReleaseInfo(
            release_date=date(2023, 10, 1),
            number_of_associations=1000,
            number_of_studies=500,
            number_of_sumstats=2000,
            number_of_snps=3000,
            ensembl_build="114",
            efo_version="EFO_2.0",
            dbsnp_build="dbSNP_151",
            gene_build="GRCh38.p13",
        )
        assert release_info.release_date == date(2023, 10, 1)
        assert release_info.number_of_associations == 1000
        assert release_info.number_of_studies == 500
        assert release_info.number_of_sumstats == 2000
        assert release_info.number_of_snps == 3000
        assert release_info.ensembl_build == "114"
        assert release_info.efo_version == "EFO_2.0"
        assert release_info.dbsnp_build == "dbSNP_151"
        assert release_info.gene_build == "GRCh38.p13"

    @pytest.mark.asyncio
    @patch("gentroutils.tasks.aiohttp.ClientSession")
    async def test_fetch_release_info(self, mock_session):
        """Test fetching release information from a URI."""
        mock_response = MagicMock()
        mock_response.json = AsyncMock(
            return_value={
                "date": "2023-10-01",
                "associations": 1000,
                "studies": 500,
                "sumstats": 2000,
                "snps": 3000,
                "ensemblbuild": "114",
                "dbsnpbuild": "dbSNP_151",
                "efoversion": "EFO_2.0",
                "genebuild": "GRCh38.p13",
            }
        )
        mock_session.return_value.__aenter__.return_value.get.return_value = mock_response

        release_info = await GwasCatalogReleaseInfo._get_release_info("http://example.com/release_info")
        assert release_info.release_date == date(2023, 10, 1)
        assert release_info.number_of_associations == 1000
        assert release_info.number_of_studies == 500
        assert release_info.number_of_sumstats == 2000
        assert release_info.number_of_snps == 3000
        assert release_info.ensembl_build == "114"
        assert release_info.dbsnp_build == "dbSNP_151"
        assert release_info.efo_version == "EFO_2.0"
        assert release_info.gene_build == "GRCh38.p13"
