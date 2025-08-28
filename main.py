# Re-export the Cloud Function entrypoint from our package path
from gcp.function_update.main import run_update  # noqa: F401
