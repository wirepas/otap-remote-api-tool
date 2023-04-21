#!/usr/bin/env python3

# node_db_converter.py - A tool to convert node addresses between
#                        a node database and a text file
#
# v0.10 2023-04-20
# Initial version
#
# Requires:
#   - Python v3.7 or newer

import sys
import argparse

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
        description="A tool to convert node addresses "
        "between a node database and a text file",
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

    parser.add_argument("-v", "--verbose", action="store_true", help="verbose output")

    args = parser.parse_args()

    if int(args.add) + int(args.delete) + int(args.to_text) != 1:
        parser.error("must specify one of --add, --delete or --to_text")

    # DEBUG: Show parsed command line arguments
    if False:
        print_msg(repr(args))


def convert():
    """Convert between a node database and a text file"""

    close_text_file = not not args.text_file
    text_file = None

    try:
        # Open node database
        print_verbose(f"opening node database {args.db_file}")
        db = node_db.NodeDb(args.db_file)

        if args.to_text:
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
                text_file.write(f"{node[0]}\n")
                num_nodes += 1

            # Print summary
            print_verbose(f"{num_nodes} node addresses written")
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
    finally:
        if close_text_file and text_file:
            text_file.close()


def main():
    """Main program"""

    parse_arguments()

    convert()


# Run main
if __name__ == "__main__":
    sys.exit(main())
