from collections import namedtuple
import requests
from rich import print
from bs4 import BeautifulSoup
import sys

t = namedtuple('p', ('name', 'signature', 'docstring'))


def gen(page, l):
    template = f'''
"""
https://data-apis.org/array-api/latest/API_specification/{page}.html
"""
from rbc.stdlib import Expose

__all__ = [{', '.join([f"'{func.name}'" for func in l])}]

expose = Expose(globals(), "{page}")
'''

    for func in l:
        template += f'''

@expose.not_implemented("{func.name}")
def _array_api_{func.name}{func.signature}:
    """
    {func.docstring}
    """
    pass
'''
    return template.lstrip()


if __name__ == '__main__':
    page = sys.argv[1]
    link = f"https://data-apis.org/array-api/latest/API_specification/{page}.html"
    r = requests.get(link)
    html = BeautifulSoup(r.content, 'html.parser')
    l = []
    for c in html.article.table.tbody:
        if c == '\n':
            continue
        name = c.contents[0].p.a.span.contents[0]
        sig = c.contents[0].p.contents[-1]
        docstring = c.contents[2].p.contents[0].strip()
        l.append(t(name, sig, docstring))

    print(gen(page, l))