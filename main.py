# main.py  (project root) — convenience entry point
#
# Setup (one-time, in your virtualenv):
#   pip install -e .
#
# Run:
#   python main.py
#   navamesh-bridge        (after pip install -e .)

from navamesh._bridge import main

if __name__ == "__main__":
    main()
