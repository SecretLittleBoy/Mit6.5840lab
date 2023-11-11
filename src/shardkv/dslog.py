#!/usr/bin/env python3
import sys
import shutil
from typing import Optional, List, Tuple, Dict

import typer
from rich import print
from rich.columns import Columns
from rich.console import Console
from rich.traceback import install


def main(
    file: typer.FileText = typer.Argument(
        None, help="File to read, stdin otherwise"),
    n_columns: Optional[int] = typer.Option(None, "--columns", "-c"),
):

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    n_columns = n_columns if n_columns else 9

    console = Console()
    width = console.size.width

    panic = False
    for line in input_:
        line = line.replace("[", "(")
        line = line.replace("]", ")")
        try:
            data, time, server, *msg = line.split(' ')
            msg = " ".join(msg)
            msg = server + " " + msg
            if (server == "(100:0)"):
                color = "green"
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                cols[0] = ""+msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1,
                               equal=True, expand=True)
                print(cols)
            elif (server == "(100:1)"):
                color = "blue"
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                cols[1] = ""+msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1,
                               equal=True, expand=True)
                print(cols)
            elif (server == "(100:2)"):
                color = "red"
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                cols[2] = ""+msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1,
                               equal=True, expand=True)
                print(cols)
            elif (server == "(101:0)"):
                color = "green"
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                cols[3] = ""+msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1,
                               equal=True, expand=True)
                print(cols)
            elif (server == "(101:1)"):
                color = "blue"
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                cols[4] = ""+msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1,
                               equal=True, expand=True)
                print(cols)
            elif (server == "(101:2)"):
                color = "red"
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                cols[5] = ""+msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1,
                               equal=True, expand=True)
                print(cols)
            elif (server == "(102:0)"):
                color = "green"
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                cols[6] = ""+msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1,
                               equal=True, expand=True)
                print(cols)
            elif (server == "(102:1)"):
                color = "blue"
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                cols[7] = ""+msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1,
                               equal=True, expand=True)
                print(cols)
            elif (server == "(102:2)"):
                color = "red"
                msg = f"[{color}]{msg}[/{color}]"
                cols = ["" for _ in range(n_columns)]
                cols[8] = ""+msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1,
                               equal=True, expand=True)
                print(cols)
            else:
                color = "white"
                msg = f"[{color}]{line}[/{color}]"
                print(msg)
        except:
            color = "white"
            msg = data + time + server + " " + msg
            msg = f"[{color}]{line}[/{color}]"
            print(msg)

if __name__ == "__main__":
    typer.run(main)
