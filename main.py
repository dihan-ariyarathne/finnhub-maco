# main.py  (root)
# Purpose: expose the Cloud Function entry point while letting GCF see the whole repo (incl. pipeline/)
from gcp.function_update.main import run_update  # re-export entry point for Cloud Functions
