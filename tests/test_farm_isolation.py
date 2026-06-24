from contextlib import contextmanager

import pytest

from navamesh.config import normalize_farm_id
from navamesh.mqtt_to_db import FARM_CLOUD_TABLES, NodeState, PostgresWriter


class RecordingConnection:
    def __init__(self):
        self.statements = []

    @contextmanager
    def cursor(self):
        connection = self

        class Cursor:
            def execute(self, sql, params=None):
                connection.statements.append((sql, params))

        yield Cursor()


def _write(table, location):
    writer = PostgresWriter("unused", table)
    writer._conn = RecordingConnection()
    writer._postgis = False
    writer.upsert_node(
        NodeState(node_id="shared-node", last_seen_ts=1, lat=1.0, lon=2.0),
        location,
        "field-node",
    )
    return writer._conn.statements[-1]


def test_local_and_cloud_writers_target_only_their_allowlisted_tables():
    local_sql, _ = _write("mesh_nodes", "FAU Garden")
    farm1_sql, _ = _write("mesh_nodes_farm1", "FAU Garden")
    farm2_sql, _ = _write("mesh_nodes_farm2", "Spirit Farm")

    assert "INSERT INTO mesh_nodes (" in local_sql
    assert "INSERT INTO mesh_nodes_farm1 (" in farm1_sql
    assert "INSERT INTO mesh_nodes_farm2 (" in farm2_sql


def test_same_node_id_is_written_to_independent_farm_tables():
    farm1_sql, farm1_params = _write("mesh_nodes_farm1", "FAU Garden")
    farm2_sql, farm2_params = _write("mesh_nodes_farm2", "Spirit Farm")

    assert farm1_params[0] == farm2_params[0] == "shared-node"
    assert "mesh_nodes_farm1" in farm1_sql
    assert "mesh_nodes_farm2" in farm2_sql
    assert "FAU Garden" in farm1_params[-1]
    assert "Spirit Farm" in farm2_params[-1]


@pytest.mark.parametrize(
    ("configured", "canonical", "table"),
    [
        ("farm1", "farm1", "mesh_nodes_farm1"),
        ("farm_1", "farm1", "mesh_nodes_farm1"),
        ("farm2", "farm2", "mesh_nodes_farm2"),
        ("farm_2", "farm2", "mesh_nodes_farm2"),
    ],
)
def test_farm_ids_normalize_to_the_correct_cloud_table(configured, canonical, table):
    assert normalize_farm_id(configured) == canonical
    assert FARM_CLOUD_TABLES[canonical] == table


def test_unknown_farm_and_arbitrary_table_fail_safely():
    with pytest.raises(ValueError, match="Unknown FARM_ID"):
        normalize_farm_id("farm3")
    with pytest.raises(ValueError, match="not allowlisted"):
        PostgresWriter("unused", "mesh_nodes; DROP TABLE mesh_nodes")
