# script to generate diffs
import argparse
import curses
import difflib
import re
import subprocess
from typing import Optional, Sequence, Tuple

import pandas as pd
import sqlparse
import termcolor

curses.setupterm()

if curses.tigetnum("colors") < 8:
    raise Exception(
        "This diff tool needs colors. Your terminal does not support enough colors"
    )

DEFAULT_DATABASE = "default"
DEFAULT_CH_HOST = "localhost"
DEFAULT_CH_PORT = 9000


class ColoredText:
    def __init__(self, color: str) -> None:
        self.color = color

    def __call__(self, text: str) -> str:
        return termcolor.colored(text, self.color)


# https://stackoverflow.com/questions/32500167/how-to-show-diff-of-two-string-sequences-in-colors

red = ColoredText("red")
green = ColoredText("green")
blue = ColoredText("blue")
white = ColoredText("white")


def get_edits_string(old: str, new: str, clip: bool = True, padding: int = 100) -> str:
    result = ""
    codes = difflib.SequenceMatcher(
        a=old, b=new, isjunk=lambda x: x in " \t\n", autojunk=False
    ).get_opcodes()

    # endpoints for clipping
    start, end = float("inf"), 0

    for code in codes:
        if code[0] == "equal":
            result += white(text=old[code[1] : code[2]])
        elif code[0] == "delete":
            result += red(text=old[code[1] : code[2]])
            start = min(len(result), start)
            end = max(len(result), end)
        elif code[0] == "insert":
            result += green(text=new[code[3] : code[4]])
            start = min(len(result), start)
            end = max(len(result), end)
        elif code[0] == "replace":
            result += red(text=old[code[1] : code[2]]) + green(
                text=new[code[3] : code[4]]
            )
            start = min(len(result), start)
            end = max(len(result), end)
        else:
            raise ValueError("Unknown code: %s" % code[0])

    if clip:
        return result[
            max(0, int(start) - 1 - padding) : min(end + padding, len(result))
        ]
    else:
        return result


def get_all_local_schema_from_clickhouse(
    host: str, port: int, database: str
) -> Sequence[Tuple[str, str]]:
    from clickhouse_driver import Client

    client = Client(
        host=host,
        port=port,
    )
    rows = client.execute(
        f"select name, create_table_query from system.tables where database = '{database}'"
    )
    return [(x[0], x[1]) for x in rows]


def get_local_schema_from_clickhouse(
    host: str, port: int, database: str, table_name: str
) -> str:
    from clickhouse_driver import Client

    client = Client(
        host=host,
        port=9000,
    )
    table: str = client.execute(
        f"select create_table_query from system.tables where database = '{database}' AND name = '{table_name}'"
    )
    if not table:
        return ""
    return table[0][0]


def ch_format(sql: str) -> str:
    if not sql:
        return ""
    p = subprocess.Popen(
        ["clickhouse-format", "--query", f"{sql};"],
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    out, err = p.communicate()
    if err:
        print("error formatting with clickhouse-format: ", err.decode())
        return sql
    return out.decode()


def main(
    schemas_csv: str,
    saas_col: str,
    local_col: str,
    groups: Optional[Sequence[str]] = None,
    ch_host: Optional[str] = None,
    ch_port: Optional[int] = None,
    ch_database: Optional[str] = None,
    clip: bool = False,
    padding: int = 100,
) -> None:

    df = pd.read_csv(schemas_csv, keep_default_na=False)

    for i, row in df.iterrows():
        if str(row["table"]).strip() != "nan":
            table_name = row["table"]
            if groups:
                matching_groups = [x for x in groups if x in table_name]
                if not matching_groups:
                    continue

            saas_query = (
                str(row[saas_col])
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("  ", " ")
                .strip()
            )
            if not local_col:
                assert (
                    ch_host and ch_port and ch_database
                ), "Must provide clickhouse host, port, database if not using local col in the schemas csv"
                local_query = get_local_schema_from_clickhouse(
                    ch_host, ch_port, ch_database, table_name
                )
            else:
                local_query = str(row[local_col])
            local_query = (
                local_query.replace("\n", " ")
                .replace("\r", " ")
                .replace("  ", " ")
                .strip()
            )

            local_query = re.sub("\s+", " ", local_query).strip()
            local_query = sqlparse.format(local_query, reindent_aligned=True)
            local_query = ch_format(local_query)

            saas_query = re.sub("\s+", " ", saas_query).strip()
            saas_query = sqlparse.format(saas_query, reindent_aligned=True)
            saas_query = ch_format(saas_query)

            if not local_query:
                print(table_name, "is missing in local", "\n")
                continue

            if not saas_query:
                print(table_name, "is missing in saas", "\n")
                continue

            if saas_query != local_query:
                print(table_name, "is different", "\n")
                print(
                    get_edits_string(
                        saas_query, local_query, clip=clip, padding=padding
                    )
                )
            else:
                print(table_name, "is same", "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Snuba Clickhouse Schema Diff Tool")
    parser.add_argument(
        "--schemas_csv",
        required=True,
        help="CSV containing the create table statements for SaaS and local (Optional).",
    )
    parser.add_argument(
        "--saas-col",
        required=True,
        help="Column name in the CSV containing the SaaS create table statement.",
    )
    parser.add_argument(
        "--local-col",
        required=False,
        help="Column name in the CSV containing the local create table statement. If not provided, the tool will use the Clickhouse client to get the schema.",
    )
    parser.add_argument(
        "--groups",
        default=None,
        help="list of groups to compare, comma separated",
    )

    parser.add_argument(
        "--ch-database",
        help="clickhouse database",
        default=DEFAULT_DATABASE,
    )
    parser.add_argument(
        "--ch-host",
        required=False,
        default=DEFAULT_CH_HOST,
        help="local clickhouse host",
    )
    parser.add_argument(
        "--ch-port",
        required=False,
        default=DEFAULT_CH_PORT,
        help="local clickhouse port",
    )
    parser.add_argument(
        "--clip",
        action="store_true",
        help="show diffs only around the smallest region that has all the differences.",
    )
    parser.add_argument(
        "--padding",
        required=False,
        default=100,
        type=int,
        help="number of characters to show around diffs when clipping",
    )

    args = parser.parse_args()
    main(
        args.schemas_csv,
        args.saas_col,
        args.local_col,
        args.groups.split(",") if args.groups else None,
        ch_database=args.ch_database,
        ch_host=args.ch_host,
        ch_port=args.ch_port,
        clip=args.clip,
        padding=args.padding,
    )
