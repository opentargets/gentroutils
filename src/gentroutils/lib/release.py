from gentropy
from __future__ import annotations
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import Window
from pyspark.sql import DataFrame
from gentropy.common.session import Session
from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.variant_index import VariantIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_prediction import L2GPrediction
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.dataset import Dataset

from functools import cached_property, cache
import polars as pl
import seaborn as sb

class OTGRelease:
    def __init__(self, prefix: str) -> None:
        if not prefix.startswith("gs://"):
            raise ValueError("Release prefix has to be a google cloud path")
        self._prefix = prefix[:-1] if prefix.endswith("/") else prefix
        self._session = session

    @cached_property
    def version(self) -> str:
        return self._prefix.split("/")[-1]

    @cached_property
    def release_paths(self) -> dict[str, str]:
        return {
            "biosample_index": f"{self._prefix}/biosample_index/",
            "coloc": f"{self._prefix}/colocalisation/coloc/",
            "ecaviar": f"{self._prefix}/colocalisation/ecaviar/",
            "credible_set": f"{self._prefix}/credible_set/",
            "gene_index": f"{self._prefix}/gene_index/",
            "invalid_credible_set": f"{self._prefix}/invalid_credible_set/",
            "invalid_study_index": f"{self._prefix}/invalid_study_index/",
            "locus_to_gene_associations": f"{self._prefix}/locus_to_gene_associations/",
            "locus_to_gene_evidence": f"{self._prefix}/locus_to_gene_evidence/",
            "locus_to_gene_feature_matrix": f"{self._prefix}/locus_to_gene_feature_matrix/",
            "locus_to_gene_model": f"{self._prefix}/locus_to_gene_model/",
            "locus_to_gene_predictions": f"{self._prefix}/locus_to_gene_predictions/",
            "study_index": f"{self._prefix}/study_index/",
            "variant_index": f"{self._prefix}/variant_index/",
            "annotated_variants": f"{self._prefix}/variants/annotated_variants/",
            "partitoned_variants": f"{self._prefix}/variants/partitioned_variants/",
        }

    @cached_property
    def release_datasets(self) -> dict[str, Dataset]:
        return {
            # indices
            "biosample_index": BiosampleIndex,
            "invalid_study_index": StudyIndex,
            "study_index": StudyIndex,
            "gene_index": GeneIndex,
            "variant_index": VariantIndex,
            # study locus
            "invalid_credible_set": StudyLocus,
            "credible_set": StudyLocus,
            # colocalisation
            "coloc": Colocalisation,
            "ecaviar": Colocalisation,
            # locus to gene
            "locus_to_gene_predictions": L2GPrediction,
        }

    def get(
        self,
        dataset_name: str,
        gentropy_session: Session,
    ) -> Dataset:
        dataset = self.release_datasets.get(dataset_name)
        if dataset is None:
            raise ValueError(f"Dataset {dataset_name} not found")
        return dataset.from_parquet(gentropy_session, self.release_paths[dataset_name])

    @cache
    def count_dataset_rows(self, session: Session) -> dict[str, int]:
        return {
            dataset_name: self.get(dataset_name, session).df.count()
            for dataset_name in self.release_datasets.keys()
        }

    def __repr__(self) -> str:
        return f"OTGRelease(prefix={self._prefix})"
