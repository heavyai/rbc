import numpy as np
from numba.targets import mathimpl

mathimpl.unary_math_extern(np.exp2, "exp2f", "exp2")
mathimpl.unary_math_extern(np.log2, "log2f", "log2")