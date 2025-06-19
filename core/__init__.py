# Shim package providing backward-compatible imports
import importlib, sys, os
parent = os.path.dirname(os.path.dirname(__file__))
if parent not in sys.path:
    sys.path.insert(0, parent)

def __getattr__(name):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        raise AttributeError(f"module 'core' has no attribute '{name}'")