#!/usr/bin/env python3

# node_db.py - An SQLite3 database for node information

from enum import Enum
import sqlite3


# Node information table definition
CREATE_QUERY = """
CREATE TABLE IF NOT EXISTS nodes(
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
    st_blob BLOB
)
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
    st_blob
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
    :st_blob
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
    st_blob = coalesce(:st_blob, st_blob)
"""

# Delete node from node information table
DELETE_QUERY = """
DELETE FROM nodes WHERE node_addr = :node_addr
"""

# Find a specific node in node information table
FIND_QUERY = """
SELECT * FROM nodes WHERE node_addr = :node_addr
"""

# Iterate over all nodes in node information table
ITER_QUERY = """
"SELECT * FROM nodes ORDER BY node_addr"
"""

# Count number of nodes in node information table
COUNT_QUERY = """
"SELECT COUNT(*) AS count FROM nodes"
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
        self.cursor = self.conn.cursor()
        self.res = self.cursor.execute(ITER_QUERY)
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

        # Create a table of nodes if it doesn't exist already
        cursor = self.conn.cursor()
        cursor.execute(CREATE_QUERY)

        self.transaction_cursor = None  # No transaction open, yet

    def get_number_of_nodes(self):
        cursor = self.conn.cursor()
        res = cursor.execute(COUNT_QUERY)
        return (res.fetchone() or {"count": 0})[
            "count"
        ]  # Just in case None is returned

    def find_node(self, node_addr):
        cursor = self.conn.cursor()
        res = cursor.execute(FIND_QUERY, {"node_addr": node_addr})
        row = res.fetchone()
        return row

    def iterator(self):
        return _NodeIterator(self.conn)

    def open_transaction(self):
        if self.transaction_cursor:
            raise ValueError("transaction already open")

        self.transaction_cursor = self.conn.cursor()
        self.transaction_cursor.execute("BEGIN TRANSACTION")

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
        st_blob=None,
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
            "st_blob": st_blob,
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
