
# Expose a temporary prototype. It will be replaced by proper
# implementation soon.
from .remotenumba import get_llvm_ir, compile

from .remotejit import RemoteJIT
