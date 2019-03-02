# Expose a temporary prototype. It will be replaced by proper
# implementation soon.
from .remotenumba import get_llvm_ir, compile   # noqa: F401

from .remotejit import RemoteJIT  # noqa: F401
