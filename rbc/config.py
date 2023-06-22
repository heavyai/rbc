import os


DEBUG = int(os.environ.get("RBC_DEBUG", False))
DEBUG_NRT = int(os.environ.get("RBC_DEBUG_NRT", False))
ENABLE_NRT = int(os.environ.get("RBC_ENABLE_NRT", True))
