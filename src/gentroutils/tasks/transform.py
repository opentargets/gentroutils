"""Module to handle gentropy transform steps via a managed Spark session."""

from __future__ import annotations

import importlib
from typing import Any, Self

from loguru import logger
from otter.task.model import Spec, Task, TaskContext
from otter.task.task_reporter import report

_STEP_REGISTRY: dict[str, tuple[str, str]] = {
    "biosample_index": ("gentropy.biosample_index", "BiosampleIndexStep"),
    "colocalisation": ("gentropy.colocalisation", "ColocalisationStep"),
    "credible_set_validation": ("gentropy.study_locus_validation", "StudyLocusValidationStep"),
    "enhancer_to_gene": ("gentropy.intervals", "IntervalE2GStep"),
    "locus_to_gene": ("gentropy.l2g", "LocusToGeneStep"),
    "study_validation": ("gentropy.study_validation", "StudyValidationStep"),
    "variant_index": ("gentropy.variant_index", "VariantIndexStep"),
    "variant_to_vcf": ("gentropy.variant_index", "ConvertToVcfStep"),
}


class PysparkTaskSpec(Spec):
    """Configuration for a gentropy transform step.

    Input paths are declared in `source`, output paths in `destination`, and
    all remaining non-path step parameters in `settings`.  Spark tuning knobs
    go in `spark_properties` (forwarded as `extended_spark_conf`), Hail knobs
    in `hail_properties` (forwarded as `extended_hail_conf`), and any other
    `gentropy.common.session.Session` constructor flags in `session_properties`.

    Examples:
    ---
    >>> ts = PysparkTaskSpec(
    ...     name="pyspark biosample_index",
    ...     pyspark="biosample_index",
    ...     source={
    ...         "cell_ontology_input_path": "gs://bucket/cl.owl",
    ...         "uberon_input_path": "gs://bucket/uberon.owl",
    ...         "efo_input_path": "gs://bucket/efo.owl",
    ...     },
    ...     destination={"biosample_index_path": "gs://bucket/output/biosample_index"},
    ...     session_properties={"write_mode": "overwrite"},
    ... )
    >>> ts.pyspark
    'biosample_index'
    >>> ts.source["uberon_input_path"]
    'gs://bucket/uberon.owl'
    >>> ts.destination
    {'biosample_index_path': 'gs://bucket/output/biosample_index'}
    >>> ts.spark_properties
    {}
    >>> ts.hail_properties
    {}
    """

    pyspark: str
    """Gentropy step name. One of: biosample_index, colocalisation,
    credible_set_validation, enhancer_to_gene, locus_to_gene, study_validation,
    variant_index, variant_to_vcf."""

    source: dict[str, Any] = {}
    """Input path parameters forwarded to the step constructor by name."""

    destination: dict[str, Any] = {}
    """Output path parameters forwarded to the step constructor by name."""

    settings: dict[str, Any] = {}
    """Non-path step parameters forwarded to the step constructor by name."""

    spark_properties: dict[str, str] = {}
    """Spark configuration entries forwarded as `extended_spark_conf` to Session."""

    hail_properties: dict[str, Any] = {}
    """Hail configuration entries forwarded as `extended_hail_conf` to Session."""

    session_properties: dict[str, Any] = {}
    """Remaining Session constructor flags (e.g. spark_uri, write_mode, output_partitions)."""


class PysparkTask(Task):
    """Task that runs a gentropy step with a managed Spark session."""

    def __init__(self, spec: PysparkTaskSpec, context: TaskContext) -> None:
        super().__init__(spec, context)
        self.spec: PysparkTaskSpec

    @report
    def run(self) -> Self:
        """Instantiate the requested gentropy step and execute it."""
        from gentropy.common.session import Session

        step_name = self.spec.pyspark
        if step_name not in _STEP_REGISTRY:
            raise ValueError(
                f"Unknown gentropy step: {step_name!r}. Available: {sorted(_STEP_REGISTRY)}"
            )

        module_name, class_name = _STEP_REGISTRY[step_name]
        logger.info(f"Running gentropy step {step_name!r} ({module_name}.{class_name})")

        step_cls = getattr(importlib.import_module(module_name), class_name)
        session = Session(
            extended_spark_conf=self.spec.spark_properties or None,
            extended_hail_conf=self.spec.hail_properties or None,
            **self.spec.session_properties,
        )
        step_cls(
            session=session,
            **self.spec.source,
            **self.spec.destination,
            **self.spec.settings,
        )

        logger.success(f"Gentropy step {step_name!r} completed.")
        return self
