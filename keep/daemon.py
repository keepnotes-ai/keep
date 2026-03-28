"""Daemon entry point: python -m keep.daemon --store PATH

Minimal argument parsing — no typer, no CLI framework.
Starts a Keeper and runs the daemon loop directly.
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(description="keep daemon")
    parser.add_argument("--store", required=True, help="Store path")
    args = parser.parse_args()

    from .api import Keeper
    kp = Keeper(store_path=args.store)
    from .cli import run_pending_daemon
    run_pending_daemon(kp)


if __name__ == "__main__":
    main()
