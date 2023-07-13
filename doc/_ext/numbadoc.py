"""
Based on https://github.com/numba/numba/issues/5755
"""
from typing import List, Optional
from sphinx.ext.autodoc import FunctionDocumenter
from numba.core.extending import _Intrinsic as Intrinsic


class RBCDocumenter(FunctionDocumenter):
    """Sphinx directive that also understands numba intrinsic decorator"""

    objtype = 'function'
    directivetype = 'function'

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        if isinstance(member, Intrinsic):
            return True
        return super(RBCDocumenter, cls).can_document_member(member, membername, isattr, parent)

    def add_directive_header(self, sig: str) -> None:
        super().add_directive_header(sig)

    def add_content(self,
                    more_content: Optional[List[str]],
                    ) -> None:
        super().add_content(more_content)


def setup(app):
    """Setup Sphinx extension."""

    # Register the new documenters and directives (autojitfun, autovecfun)
    # Set the default prefix which is printed in front of the function signature
    app.setup_extension('sphinx.ext.autodoc')
    app.add_autodocumenter(RBCDocumenter)

    return {
        'parallel_read_safe': True
    }
