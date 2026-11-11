"""Test cases for the PysparkTask task."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from otter.task.model import State, TaskContext

from gentroutils.tasks.transform import PysparkTask, PysparkTaskSpec, _STEP_REGISTRY


@pytest.fixture
def mock_task_context():
    """Return a mock TaskContext."""
    context = Mock(spec=TaskContext)
    context.state = State.PENDING_RUN
    context.abort = Mock()
    context.abort.set = Mock()
    return context


@pytest.fixture
def biosample_spec(mock_task_context):
    """Return a minimal PysparkTaskSpec for the biosample_index step."""
    return PysparkTaskSpec(
        name="pyspark biosample_index",
        pyspark="biosample_index",
        source={
            "cell_ontology_input_path": "gs://bucket/cl.json",
            "uberon_input_path": "gs://bucket/uberon.json",
            "efo_input_path": "gs://bucket/efo.json",
        },
        destination={"biosample_index_path": "gs://bucket/output/biosample"},
        session_properties={"write_mode": "overwrite", "output_partitions": 1},
    )


class TestPysparkTaskSpec:
    """Tests for PysparkTaskSpec field defaults and validation."""

    def test_required_pyspark_field(self):
        """pyspark is required; omitting it raises a validation error."""
        with pytest.raises(Exception):
            PysparkTaskSpec(name="pyspark biosample_index")

    def test_optional_fields_default_to_empty_dicts(self):
        """All optional dict fields default to empty dicts."""
        spec = PysparkTaskSpec(name="pyspark biosample_index", pyspark="biosample_index")
        assert spec.source == {}
        assert spec.destination == {}
        assert spec.settings == {}
        assert spec.spark_properties == {}
        assert spec.hail_properties == {}
        assert spec.session_properties == {}

    def test_all_registered_steps_are_valid_pyspark_values(self):
        """Every key in _STEP_REGISTRY is a valid pyspark field value."""
        for step_name in _STEP_REGISTRY:
            spec = PysparkTaskSpec(name=f"pyspark {step_name}", pyspark=step_name)
            assert spec.pyspark == step_name

    def test_spec_stores_source_and_destination(self, biosample_spec):
        """source and destination dicts are stored verbatim."""
        assert biosample_spec.source["cell_ontology_input_path"] == "gs://bucket/cl.json"
        assert biosample_spec.destination["biosample_index_path"] == "gs://bucket/output/biosample"

    def test_spec_stores_session_properties(self, biosample_spec):
        """session_properties are stored verbatim."""
        assert biosample_spec.session_properties["write_mode"] == "overwrite"
        assert biosample_spec.session_properties["output_partitions"] == 1


class TestPysparkTask:
    """Tests for PysparkTask.run() behaviour."""

    def test_task_initialization(self, biosample_spec, mock_task_context):
        """Task stores spec and exposes it as self.spec."""
        task = PysparkTask(biosample_spec, mock_task_context)
        assert task.spec is biosample_spec

    @patch("gentroutils.tasks.transform.importlib.import_module")
    @patch("gentropy.common.session.Session")
    def test_run_returns_self(self, _mock_session, mock_import, biosample_spec, mock_task_context):
        """run() returns the task instance."""
        mock_import.return_value = MagicMock()
        task = PysparkTask(biosample_spec, mock_task_context)
        assert task.run() is task

    @patch("gentroutils.tasks.transform.importlib.import_module")
    @patch("gentropy.common.session.Session")
    def test_run_creates_session_with_session_properties(
        self, mock_session_cls, mock_import, biosample_spec, mock_task_context
    ):
        """session_properties are unpacked into Session constructor."""
        mock_import.return_value = MagicMock()
        PysparkTask(biosample_spec, mock_task_context).run()
        mock_session_cls.assert_called_once_with(
            extended_spark_conf=None,
            extended_hail_conf=None,
            write_mode="overwrite",
            output_partitions=1,
        )

    @patch("gentroutils.tasks.transform.importlib.import_module")
    @patch("gentropy.common.session.Session")
    def test_run_passes_spark_properties_as_extended_spark_conf(
        self, mock_session_cls, mock_import, mock_task_context
    ):
        """Non-empty spark_properties are forwarded as extended_spark_conf."""
        spec = PysparkTaskSpec(
            name="pyspark colocalisation",
            pyspark="colocalisation",
            spark_properties={"spark.executor.memory": "22g"},
        )
        mock_import.return_value = MagicMock()
        PysparkTask(spec, mock_task_context).run()
        call_kwargs = mock_session_cls.call_args.kwargs
        assert call_kwargs["extended_spark_conf"] == {"spark.executor.memory": "22g"}

    @patch("gentroutils.tasks.transform.importlib.import_module")
    @patch("gentropy.common.session.Session")
    def test_run_passes_hail_properties_as_extended_hail_conf(
        self, mock_session_cls, mock_import, mock_task_context
    ):
        """Non-empty hail_properties are forwarded as extended_hail_conf."""
        spec = PysparkTaskSpec(
            name="pyspark biosample_index",
            pyspark="biosample_index",
            hail_properties={"hail.some.setting": "value"},
        )
        mock_import.return_value = MagicMock()
        PysparkTask(spec, mock_task_context).run()
        call_kwargs = mock_session_cls.call_args.kwargs
        assert call_kwargs["extended_hail_conf"] == {"hail.some.setting": "value"}

    @patch("gentroutils.tasks.transform.importlib.import_module")
    @patch("gentropy.common.session.Session")
    def test_run_converts_empty_spark_properties_to_none(
        self, mock_session_cls, mock_import, mock_task_context
    ):
        """Empty spark_properties dict is converted to None for Session."""
        spec = PysparkTaskSpec(name="pyspark biosample_index", pyspark="biosample_index")
        mock_import.return_value = MagicMock()
        PysparkTask(spec, mock_task_context).run()
        call_kwargs = mock_session_cls.call_args.kwargs
        assert call_kwargs["extended_spark_conf"] is None
        assert call_kwargs["extended_hail_conf"] is None

    @patch("gentroutils.tasks.transform.importlib.import_module")
    @patch("gentropy.common.session.Session")
    def test_run_forwards_source_destination_settings_to_step(
        self, mock_session_cls, mock_import, mock_task_context
    ):
        """source, destination, and settings are unpacked into the step constructor."""
        spec = PysparkTaskSpec(
            name="pyspark credible_set_validation",
            pyspark="credible_set_validation",
            source={"study_locus_path": ["gs://bucket/cs"], "study_index_path": "gs://bucket/study"},
            destination={"valid_study_locus_path": "gs://bucket/valid"},
            settings={"trans_qtl_threshold": 5_000_000},
        )
        mock_module = MagicMock()
        mock_step_cls = MagicMock()
        mock_module.StudyLocusValidationStep = mock_step_cls
        mock_import.return_value = mock_module

        PysparkTask(spec, mock_task_context).run()

        mock_step_cls.assert_called_once_with(
            session=mock_session_cls.return_value,
            study_locus_path=["gs://bucket/cs"],
            study_index_path="gs://bucket/study",
            valid_study_locus_path="gs://bucket/valid",
            trans_qtl_threshold=5_000_000,
        )

    @patch("gentroutils.tasks.transform.importlib.import_module")
    @patch("gentropy.common.session.Session")
    def test_run_imports_correct_module_for_step(
        self, _mock_session, mock_import, mock_task_context
    ):
        """run() imports the module registered for the given step name."""
        spec = PysparkTaskSpec(name="pyspark variant_index", pyspark="variant_index")
        mock_import.return_value = MagicMock()
        PysparkTask(spec, mock_task_context).run()
        mock_import.assert_called_once_with("gentropy.variant_index")

    def test_run_raises_for_unknown_step(self, mock_task_context):
        """run() raises ValueError when the step name is not in the registry.

        @report swallows exceptions via self.fail(); bypass it via __wrapped__
        to assert the underlying ValueError is raised.
        """
        spec = PysparkTaskSpec(name="pyspark unknown", pyspark="unknown_step")
        task = PysparkTask(spec, mock_task_context)
        with pytest.raises(ValueError, match="Unknown gentropy step"):
            task.run.__wrapped__(task)
