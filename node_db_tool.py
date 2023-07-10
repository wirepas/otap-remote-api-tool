#!/usr/bin/env python3

# node_db_tool.py - A tool to work with node information database files
#
# v0.10 2023-04-20
# Initial version
#
# Requires:
#   - Python v3.7 or newer

import sys
import os
import argparse
import tempfile
import sqlite3

import node_db


args = None


def print_msg(msg):
    """Print normal messages"""
    sys.stderr.write(msg)
    sys.stderr.write("\n")
    sys.stderr.flush()


def print_verbose(msg):
    """Print verbose messages"""
    if args.verbose:
        sys.stderr.write(msg)
        sys.stderr.write("\n")
        sys.stderr.flush()


def parse_arguments():
    """Parse command line arguments"""

    global args

    parser = argparse.ArgumentParser(
        description="A tool to work with node information database files",
        epilog="",
    )

    parser.add_argument(
        "db_file",
        type=str,
        help="node database file",
    )

    parser.add_argument(
        "text_file",
        type=str,
        nargs="?",
        help="text file (standard input or output, if not specified)",
    )

    parser.add_argument(
        "-a",
        "--add",
        action="store_true",
        help="add nodes listed in text file to database",
    )

    parser.add_argument(
        "-d",
        "--delete",
        action="store_true",
        help="delete nodes listed in text file from database",
    )

    parser.add_argument(
        "-t",
        "--to_text",
        action="store_true",
        help="convert a node database back to text",
    )

    parser.add_argument(
        "-s",
        "--seq_info",
        action="store_true",
        help="statistics about otap sequence numbers",
    )

    parser.add_argument(
        "-u",
        "--upgrade_db",
        action="store_true",
        help=f"upgrade database schema to the latest version (version {node_db.SUPPORTED_SCHEMA_VERSION})",
    )

    parser.add_argument("-v", "--verbose", action="store_true", help="verbose output")

    args = parser.parse_args()

    if (
        int(args.add)
        + int(args.delete)
        + int(args.to_text)
        + int(args.seq_info)
        + int(args.upgrade_db)
        != 1
    ):
        parser.error(
            "must specify one of --add, --delete, --to_text, --seq_info or --upgrade_db"
        )

    if args.text_file and (args.seq_info or args.upgrade_db):
        parser.error("cannot have --text_file with --seq_info or --upgrade_db")

    # DEBUG: Show parsed command line arguments
    if False:
        print_msg(repr(args))


def upgrade_schema_version():
    tempfilename = None

    try:
        # Create a temporary file in the same directory as the old SQLite3 database
        _, tempfilename = tempfile.mkstemp(
            dir=os.path.dirname(args.db_file), prefix="node_db_tool.", suffix=".sqlite3"
        )

        # Create an new, empty SQLite3 database with a temporary name
        try:
            db = node_db.NodeDb(tempfilename)
        except ValueError as exc:
            raise ValueError(f"{tempfilename}: {str(exc)}") from None

        # Open old SQLite3 database
        try:
            conn = sqlite3.connect(args.db_file)
            conn.row_factory = sqlite3.Row
        except (ValueError, sqlite3.Error) as exc:
            raise ValueError(f"{args.db_file}: {str(exc)}") from None

        # Get a list of tables in the old database
        cursor = conn.execute("SELECT * FROM sqlite_master WHERE type='table'")
        tables = [r["name"] for r in cursor]
        cursor.close()

        if len(tables) != 1:
            # Database has more than one table or no tables at all,
            # so don't know what to do
            raise ValueError(f"{args.db_file}: unsupported database schema")

        # Convert node information to the new format
        try:
            cursor = conn.execute("SELECT * FROM nodes")
            db.open_transaction()
            for row in cursor:
                node_info = dict(row)
                db.add_or_update_node(
                    node_addr=node_info["node_addr"],
                    last_seen=node_info["last_seen"],
                    phase=node_db.Phase(node_info["phase"]),
                    node_role=node_info["node_role"],
                    lock_status=node_db.OtapLockStatus(node_info["lock_status"]),
                    last_req=node_info["last_req"],
                    last_resp=node_info["last_resp"],
                    last_info=node_info["last_info"],
                    st_len=node_info["st_len"],
                    st_crc=node_info["st_crc"],
                    st_seq=node_info["st_seq"],
                    st_type=node_info["st_type"],
                    st_status=node_info["st_status"],
                )
            db.commit()
        except (ValueError, sqlite3.Error) as exc:
            raise ValueError(f"{args.db_file}: {str(exc)}") from None

        # Replace old SQLite3 database with new
        try:
            os.replace(tempfilename, args.db_file)
        except OSError as exc:
            raise ValueError(f"{args.db_file}: {str(exc)}") from None

        print_verbose(f"schema upgraded to version {node_db.SUPPORTED_SCHEMA_VERSION}")
    finally:
        if tempfile is not None:
            # Remove temporary file
            try:
                os.remove(tempfilename)
            except OSError:
                pass


def perform_command():
    """Perform database command"""

    close_text_file = not not args.text_file
    text_file = None

    try:
        # Open node database
        db = None
        print_verbose(f"opening node database {args.db_file}")

        try:
            db = node_db.NodeDb(args.db_file)

            if args.upgrade_db:
                # Opened successfully, so already the correct schema version
                print_msg(
                    f"{args.db_file}: already schema version "
                    f"{node_db.SUPPORTED_SCHEMA_VERSION}"
                )
                db = None
        except ValueError as exc:
            if args.upgrade_db:
                # Upgrade the database schema version
                db = None
                upgrade_schema_version()
            else:
                raise ValueError(f"{args.db_file}: {str(exc)}") from None

        if db is None:
            # Command already handled above
            pass
        elif args.to_text:
            # Convert node database to text
            if args.text_file:
                text_file = open(args.text_file, "w")
                print_verbose(f"writing to text file {args.text_file}")
            else:
                text_file = sys.stdout
                print_verbose(f"writing to stdout")

            # Iterate through nodes
            num_nodes = 0
            for node in db.iterator():
                text_file.write(f"{node['node_addr']}\n")
                num_nodes += 1

            # Print summary
            print_verbose(f"{num_nodes} node addresses written")
        elif args.seq_info:
            info = db.seq_info()
            seqs = list(info.items())
            seqs.sort(key=lambda i: i[1], reverse=True)
            sys.stdout.write(f"number of nodes: {db.get_number_of_nodes()}\n")
            sys.stdout.write("seq\tcount\n")
            for seq in seqs:
                seq_name = (seq[0] is not None) and f"{seq[0]:3d}" or "???"
                sys.stdout.write(f"{seq_name}\t{seq[1]}\n")
        else:
            # Convert text to node database
            if args.text_file:
                text_file = open(args.text_file, "r")
                print_verbose(f"reading from text file {args.text_file}")
            else:
                text_file = sys.stdin
                print_verbose(f"reading from stdin")

            # Open a transaction, in case of errors
            db.open_transaction()

            for line in text_file:
                # Remove comments, commas, whitespace
                line = line.replace(";", "#")
                line = line[: line.find("#")]
                line = line.replace(",", " ").strip()

                if not line:
                    continue

                # Insert all nodes in database
                for node_addr in [int(s) for s in line.split()]:
                    if args.add:
                        db.add_or_update_node(node_addr)
                    else:
                        db.delete_node(node_addr)

            # Commit changes back to disk
            db.commit()

            # Print summary
            print_verbose(f"{db.get_number_of_nodes()} nodes in database")
    except ValueError as exc:
        print_msg(str(exc))
    finally:
        if close_text_file and text_file:
            text_file.close()


def main():
    """Main program"""

    parse_arguments()

    perform_command()


# Run main
if __name__ == "__main__":
    sys.exit(main())
