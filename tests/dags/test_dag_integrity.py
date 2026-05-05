"""
Test the validity of all DAGs.

Checks performed:
  - No import errors on any DAG file
  - All DAGs have at least one tag
  - All DAGs have retries >= 2 in default_args
  - All DAGs have at least one task (basic structure check)
  - All pool-using tasks reference a known pool name

Airflow 3 Migration Notes:
  - DagBag constructor: include_examples=False still supported in Airflow 3.
  - DagBag.import_errors: dict structure unchanged.
  - DagBag.dags: dict structure unchanged in Airflow 3.0; note that Airflow 3.2
    changes what DagBag returns — guard with hasattr checks where needed.
  - APPROVED_TAGS left empty (allow any tags) to keep the test flexible.
  - Added test_dag_has_tasks and test_dag_pool_slots as new Airflow 3-era checks.
"""

import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag.
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME", "."))

    # prepend "(None, None)" to ensure a test object is always created
    return [(None, None)] + [
        (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
    ]


def get_dags():
    """
    Generate a tuple of (dag_id, DAG object, relative_file_path) from the DagBag.
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME", "."))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


# ---------------------------------------------------------------------------
# Test: no import errors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "rel_path,rv",
    get_import_errors(),
    ids=[x[0] for x in get_import_errors()],
)
def test_file_imports(rel_path, rv):
    """Fail if any DAG file has an import error."""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message:\n{rv}")


# ---------------------------------------------------------------------------
# Test: all DAGs have tags
# ---------------------------------------------------------------------------

APPROVED_TAGS = {}  # Empty = allow any tags; populate to restrict


@pytest.mark.parametrize(
    "dag_id,dag,fileloc",
    get_dags(),
    ids=[x[2] for x in get_dags()],
)
def test_dag_tags(dag_id, dag, fileloc):
    """All DAGs must have at least one tag."""
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS, (
            f"{dag_id} in {fileloc} uses unapproved tags: "
            f"{set(dag.tags) - APPROVED_TAGS}"
        )


# ---------------------------------------------------------------------------
# Test: all DAGs have retries >= 2
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dag_id,dag,fileloc",
    get_dags(),
    ids=[x[2] for x in get_dags()],
)
def test_dag_retries(dag_id, dag, fileloc):
    """All DAGs must set retries >= 2 in default_args."""
    retries = dag.default_args.get("retries", None)
    assert retries is not None and retries >= 2, (
        f"{dag_id} in {fileloc} does not have retries set to >= 2 "
        f"(current value: {retries})"
    )


# ---------------------------------------------------------------------------
# Test: all DAGs have at least one task  [NEW — Airflow 3]
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dag_id,dag,fileloc",
    get_dags(),
    ids=[x[2] for x in get_dags()],
)
def test_dag_has_tasks(dag_id, dag, fileloc):
    """All DAGs must define at least one task."""
    assert len(dag.tasks) > 0, f"{dag_id} in {fileloc} has no tasks defined"


# ---------------------------------------------------------------------------
# Test: pool-using tasks reference only known pools  [NEW — Airflow 3]
# ---------------------------------------------------------------------------

KNOWN_POOLS = {"duckdb_pool", "default_pool"}


@pytest.mark.parametrize(
    "dag_id,dag,fileloc",
    get_dags(),
    ids=[x[2] for x in get_dags()],
)
def test_dag_pool_slots(dag_id, dag, fileloc):
    """Tasks that reference a named pool must use a known pool name."""
    for task in dag.tasks:
        pool = getattr(task, "pool", None)
        if pool and pool not in KNOWN_POOLS:
            raise AssertionError(
                f"{dag_id}.{task.task_id} in {fileloc} references unknown pool "
                f"'{pool}'. Known pools: {KNOWN_POOLS}"
            )
