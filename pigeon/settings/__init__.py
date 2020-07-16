
from .defaults import *

try:
    from .local import *
except ImportError:
    print('No settings file found. Did you remember to copy local-dist.py to local.py?')
