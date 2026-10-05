"""Runs the full pipeline once on locally generated TPC-H data (scale factor 0.01)."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from generate_local_data import generate  # noqa: E402
from run_local import LOCAL, LOCAL_CONFIG, local_spark  # noqa: E402

from tpch_lakehouse import bronze, gold, silver  # noqa: E402


@pytest.fixture(scope="session")
def cfg():
    return LOCAL_CONFIG


@pytest.fixture(scope="session")
def spark(cfg):
    session = local_spark()
    session.sparkContext.setLogLevel("ERROR")
    generate(session, LOCAL / "data", scale_factor=0.01)
    bronze.run(session, cfg)
    silver.run(session, cfg)
    gold.run(session, cfg)
    yield session
    session.stop()
