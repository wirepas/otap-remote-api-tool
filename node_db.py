#!/usr/bin/env python3

# node_db.py - An SQLite3 database for node information

from enum import Enum
import sqlite3


# What the database is used for
DATABASE_CREATOR = "com/wirepas/otap-tool"

# Version of supported database schema
# NOTE: Version 0 didn't have a metadata table
SUPPORTED_SCHEMA_VERSION = 1

# Metadata table definition
CREATE_METADATA_TABLE_QUERY = """
CREATE TABLE metadata(
    key TEXT PRIMARY KEY,
    value TEXT
) WITHOUT ROWID;
"""

# Node information table definition
CREATE_NODES_TABLE_QUERY = """
CREATE TABLE nodes(
    node_addr INT PRIMARY KEY,
    last_seen INT,
    phase INT,
    node_role INT,
    lock_status INT,
    last_req INT,
    last_resp INT,
    last_info INT,
    st_len INT,
    st_crc INT,
    st_seq INT,
    st_type INT,
    st_status INT,
    fw_len INT,
    fw_crc INT,
    fw_seq INT,
    fw_id INT,
    fw_ver TEXT,
    app_len INT,
    app_crc INT,
    app_seq INT,
    app_id INT,
    app_ver TEXT
);
"""

# Read metadata value
METADATA_READ_QUERY = """
SELECT * FROM metadata WHERE key = :key
"""

# Set metadata value
METADATA_WRITE_QUERY = """
INSERT INTO metadata VALUES (:key, :value)
"""

# Insert a new node, but keep or update old data if node already exists
INSERT_OR_UPDATE_QUERY = """
INSERT INTO nodes (
    node_addr,
    last_seen,
    phase,
    node_role,
    lock_status,
    last_req,
    last_resp,
    last_info,
    st_len,
    st_crc,
    st_seq,
    st_type,
    st_status,
    fw_len,
    fw_crc,
    fw_seq,
    fw_id,
    fw_ver,
    app_len,
    app_crc,
    app_seq,
    app_id,
    app_ver
) VALUES (
    :node_addr,
    :last_seen,
    :phase,
    :node_role,
    :lock_status,
    :last_req,
    :last_resp,
    :last_info,
    :st_len,
    :st_crc,
    :st_seq,
    :st_type,
    :st_status,
    :fw_len,
    :fw_crc,
    :fw_seq,
    :fw_id,
    :fw_ver,
    :app_len,
    :app_crc,
    :app_seq,
    :app_id,
    :app_ver
)
ON CONFLICT(node_addr) DO UPDATE SET
    node_addr = coalesce(:node_addr, node_addr),
    last_seen = coalesce(:last_seen, last_seen),
    phase = coalesce(:phase, phase),
    node_role = coalesce(:node_role, node_role),
    lock_status = coalesce(:lock_status, lock_status),
    last_req = coalesce(:last_req, last_req),
    last_resp = coalesce(:last_resp, last_resp),
    last_info = coalesce(:last_info, last_info),
    st_len = coalesce(:st_len, st_len),
    st_crc = coalesce(:st_crc, st_crc),
    st_seq = coalesce(:st_seq, st_seq),
    st_type = coalesce(:st_type, st_type),
    st_status = coalesce(:st_status, st_status),
    fw_len = coalesce(:fw_len, fw_len),
    fw_crc = coalesce(:fw_crc, fw_crc),
    fw_seq = coalesce(:fw_seq, fw_seq),
    fw_id = coalesce(:fw_id, fw_id),
    fw_ver = coalesce(:fw_ver, fw_ver),
    app_len = coalesce(:app_len, app_len),
    app_crc = coalesce(:app_crc, app_crc),
    app_seq = coalesce(:app_seq, app_seq),
    app_id = coalesce(:app_id, app_id),
    app_ver = coalesce(:app_ver, app_ver)
"""

# Delete node from node information table
DELETE_QUERY = """
DELETE FROM nodes WHERE node_addr = :node_addr
"""

# Find a specific node in node information table
FIND_QUERY = """
SELECT * FROM nodes WHERE node_addr = :node_addr
"""

# Find a node with the oldest request time
FIND_OLDEST_REQ_QUERY = """
SELECT node_addr FROM nodes WHERE (
  last_req is NULL OR
  last_info is NULL OR
  (:timeout != 0 AND :now - last_req >= :timeout)
)
ORDER BY last_req LIMIT 1
"""

# Iterate over all nodes in node information table
ITER_QUERY = """
SELECT * FROM nodes ORDER BY node_addr
"""

# Count number of nodes in node information table
COUNT_QUERY = """
SELECT COUNT(*) AS count FROM nodes
"""

# Statistics about sequence numbers
SEQ_INFO_QUERY = """
SELECT st_seq, COUNT(*) AS count FROM nodes GROUP BY st_seq
"""


class Phase(Enum):
    """Node phase values"""

    INIT = 0
    INFO_REQ = 1
    LOCK_UNLOCK_REQ = 2
    DONE = 3


class OtapLockStatus(Enum):
    """Node lock_status values"""

    UNLOCKED = 0
    UNLOCKED_KEY_SET = 1
    UNLOCKED_BITS_SET = 2
    LOCKED = 3


class _NodeIterator:
    """Iterator for node information"""

    def __init__(self, conn):
        self.conn = conn

    def __iter__(self):
        self.cursor = self.conn.execute(ITER_QUERY)
        return self

    def __next__(self):
        row = self.res.fetchone()
        if not row:
            raise StopIteration
        return row


class NodeDb:
    """SQLite3 node information database"""

    def __init__(self, filename):
        # Open an SQLite3 database
        self.conn = sqlite3.connect(filename)
        self.conn.row_factory = sqlite3.Row

        self.transaction_cursor = None  # No transaction open, yet

        # Create tables, if the database is empty
        self._create_tables()

        # Check that the database is something this module can use
        self._check_database_format()

    def _create_tables(self):
        # Get a list of tables in the database
        cursor = self.conn.execute("SELECT * FROM sqlite_master WHERE type='table'")
        tables = [r["name"] for r in cursor]
        cursor.close()

        if len(tables) > 0:
            # Database not empty, do not create tables
            return

        # Create "metadata" and "nodes" tables
        self.conn.execute(CREATE_METADATA_TABLE_QUERY).close()
        self.conn.execute(CREATE_NODES_TABLE_QUERY).close()

        # Populate "metadata" table
        transaction_cursor = self.conn.execute("BEGIN TRANSACTION")
        row = {"key": "creator", "value": DATABASE_CREATOR}
        transaction_cursor.execute(METADATA_WRITE_QUERY, row)
        row = {"key": "schema_version", "value": SUPPORTED_SCHEMA_VERSION}
        transaction_cursor.execute(METADATA_WRITE_QUERY, row)
        transaction_cursor.execute("COMMIT TRANSACTION")

    def _check_database_format(self):
        try:
            # Get database creator
            cursor = self.conn.execute(METADATA_READ_QUERY, {"key": "creator"})
            db_creator = cursor.fetchone()["value"]
            cursor.close()

            if db_creator != DATABASE_CREATOR:
                # Not the expected creator
                raise ValueError

            # Get the database schema version
            cursor = self.conn.execute(METADATA_READ_QUERY, {"key": "schema_version"})
            schema_version = int(cursor.fetchone()["value"])
            cursor.close()
        except (ValueError, TypeError, sqlite3.OperationalError):
            raise ValueError("unsupported database schema") from None

        if schema_version != SUPPORTED_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported database schema version {schema_version}"
                f", only version {SUPPORTED_SCHEMA_VERSION} supported"
            )

    def get_number_of_nodes(self):
        cursor = self.conn.execute(COUNT_QUERY)
        return (cursor.fetchone() or {"count": 0})[
            "count"
        ]  # Just in case None is returned

    def find_node(self, node_addr):
        cursor = self.conn.execute(FIND_QUERY, {"node_addr": node_addr})
        row = cursor.fetchone()
        if row is None:
            return None

        node_info = dict(row)

        # Convert value to Enum
        if node_info["phase"] is not None:
            node_info["phase"] = Phase(node_info["phase"])

        if node_info["lock_status"] is not None:
            node_info["lock_status"] = OtapLockStatus(node_info["lock_status"])

        return node_info

    def find_node_oldest_req(self, now=0, timeout=0):
        # Timeout may be zero or omitted, in which case only
        # nodes with no info are considered
        cursor = self.conn.execute(
            FIND_OLDEST_REQ_QUERY, {"now": now, "timeout": timeout}
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return row["node_addr"]

    def seq_info(self):
        cursor = self.conn.execute(SEQ_INFO_QUERY)
        seqs = {}
        for row in cursor:
            seqs[row["st_seq"]] = row["count"]
        return seqs

    def iterator(self):
        # return _NodeIterator(self.conn)
        return self.conn.execute(ITER_QUERY)

    def open_transaction(self):
        if self.transaction_cursor:
            raise ValueError("transaction already open")

        self.transaction_cursor = self.conn.execute("BEGIN TRANSACTION")

    def add_or_update_node(
        self,
        node_addr,
        last_seen=None,
        phase=None,
        node_role=None,
        lock_status=None,
        last_req=None,
        last_resp=None,
        last_info=None,
        st_len=None,
        st_crc=None,
        st_seq=None,
        st_type=None,
        st_status=None,
        fw_len=None,
        fw_crc=None,
        fw_seq=None,
        fw_id=None,
        fw_ver=None,
        app_len=None,
        app_crc=None,
        app_seq=None,
        app_id=None,
        app_ver=None,
    ):
        if not self.transaction_cursor:
            raise ValueError("no transaction open")

        # Convert Enum to value
        if phase is not None:
            phase = phase.value

        if lock_status is not None:
            lock_status = lock_status.value

        values = {
            "node_addr": node_addr,
            "last_seen": last_seen,
            "phase": phase,
            "node_role": node_role,
            "lock_status": lock_status,
            "last_req": last_req,
            "last_resp": last_resp,
            "last_info": last_info,
            "st_len": st_len,
            "st_crc": st_crc,
            "st_seq": st_seq,
            "st_type": st_type,
            "st_status": st_status,
            "fw_len": fw_len,
            "fw_crc": fw_crc,
            "fw_seq": fw_seq,
            "fw_id": fw_id,
            "fw_ver": fw_ver,
            "app_len": app_len,
            "app_crc": app_crc,
            "app_seq": app_seq,
            "app_id": app_id,
            "app_ver": app_ver,
        }

        # Create a new row or update existing row with any given non-NULL values
        self.transaction_cursor.execute(INSERT_OR_UPDATE_QUERY, values)

    def delete_node(self, node_addr):
        if not self.transaction_cursor:
            raise ValueError("no transaction open")

        self.transaction_cursor.execute(DELETE_QUERY, {"node_addr": node_addr})

    def commit(self):
        if not self.transaction_cursor:
            return

        # Commit
        self.transaction_cursor.execute("COMMIT TRANSACTION")
        self.transaction_cursor = None  # Transaction committed

    def cancel(self):
        if not self.transaction_cursor:
            return

        # Cancel
        self.transaction_cursor.execute("CANCEL TRANSACTION")
        self.transaction_cursor = None  # Transaction cancelled
