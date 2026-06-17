#!/usr/bin/env python3
"""Friendly launcher for the hybrid database framework.

Usage:
    python run.py                 # interactive hub
    python run.py --pipeline      # run the full ingest->classify->init pipeline
    python run.py dashboard       # launch the web dashboard
    python run.py simulate        # run the data-stream simulation server
    python run.py acid            # run the ACID / reliability test suite
    python run.py benchmark       # run performance benchmarks
    python run.py init-users      # create default dashboard users

Everything is also runnable directly, e.g. `python -m hybriddb.core.main`.
"""

import sys


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) > 1 else ""

    if arg == "dashboard":
        from hybriddb.dashboard.dashboard_app import main as dash_main
        dash_main()
    elif arg == "simulate":
        import uvicorn
        uvicorn.run("hybriddb.tools.simulation_code:app", host="127.0.0.1", port=8000)
    elif arg == "acid":
        from hybriddb.testing.acid_test_runner import main as acid_main
        raise SystemExit(acid_main())
    elif arg == "benchmark":
        from hybriddb.analysis.benchmark_runner import run_benchmarks
        run_benchmarks()
    elif arg == "init-users":
        from hybriddb.tools.init_users import main as users_main
        users_main()
    else:
        # Default: interactive hub (also handles --pipeline via sys.argv).
        from hybriddb.core.main import run_menu
        run_menu()


if __name__ == "__main__":
    main()
