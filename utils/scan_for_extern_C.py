import os
import re

extern_C_re = re.compile(r'(EXTENSION_INLINE|EXTENSION_NOINLINE|extern\s+"C"'
                         r'.*@@@_SUF@@FIX_@@@|extern\s+"C")([^(]+)[(]', re.M)


def process_name(src, content):
    result = set()
    if '##' in content:
        for var, variants in [
                ('##key_type', 'int8_t int16_t int32_t int64_t'),
                ('opname##', 'add sub mul div mod'),
                ('opname##', 'add sub mul div mod'),
                ('##from_type##', 'int8_t int16_t int32_t int64_t float double'),
                ('##to_type##', 'int8_t int16_t int32_t int64_t float double'),
                ('##type##', 'int8_t int16_t int32_t int64_t float double'),
                ('##type', 'int8_t int16_t int32_t int64_t float double'),
                ('##needle_type', 'int8_t int16_t int32_t int64_t float double'),
                ('##_skip_val', 'agg_min_int8 agg_max_int8 agg_min_int16 agg_max_int16'
                 ' agg_min_int32 agg_max_int32 agg_min agg_max'),
                ('base_agg_func##', 'agg_count agg_max agg_min agg_id'),
                ('##n', '8 16 32'),
                ('##oper_name##', 'eq ne lt gt le ge'),
                ('base_func##', 'string_like_simple string_ilike_simple string_like'
                 ' string_ilike string_lt string_gt string_le string_ge string_eq string_ne')]:
            for v in variants.split():
                if var[0] != '#':
                    if content.startswith(var):
                        new = v + content[len(var):]
                    else:
                        break  # no match
                elif var[-1] != '#':
                    if content.endswith(var):
                        new = content[:-len(var)] + v
                    else:
                        break  # no match
                else:
                    assert var[0] + var[-1] == '##', var
                    if var in content:
                        new = content.replace(var, v)
                    else:
                        break  # no match
                result.update(process_name(src, new))
            else:
                break  # was match
        else:
            raise NotImplementedError(repr((content, src)))
    else:
        result.add(content)
    return result


def process_file(src, content):
    result = set()

    for m in extern_C_re.findall(content):
        fname = m[-1].split()[-1].strip()
        if fname.endswith(')'):
            fname = fname[:-1].strip()
            assert 'SUF@@FIX' in ''.join(m), m
        assert 'SUFFIX' not in fname, m
        result.update(process_name(src, fname))

    return result


def process_folder(path):

    result = set()

    for root, dirs, files in os.walk(os.path.abspath(path)):
        for f in files:
            ext = os.path.splitext(f)[1]
            if ext in ['.cpp', '.h', '.hpp']:
                fn = os.path.join(root, f)
                print(fn)
                content = open(fn).read()
                content = content.replace('SUFFIX(', '@@@_SUF@@FIX_@@@ ')
                result.update(process_file(f, content))

    return result


def main():
    return
    rootdir = '/path/to/omniscidb-internal/'
    rootdir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    symbols = set()
    for folder in ['QueryEngine', 'Utils']:
        result = process_folder(os.path.join(rootdir, folder))
        symbols.update(result)

    fn = os.path.join(rootdir, 'QueryEngine', 'defined_symbols_omnisci.h')
    f = open(fn, 'w')
    f.write('''/*
    This file contains all function names that omniscidb defines.

    To be included only in NativeCodegen.cpp.

    The file is generated with script QueryEngine/scripts/scan_for_extern_C.py
*/

const std::unordered_set<std::string> extra_defined_symbols_omnisci(
    {{"''')

    f.write('",\n     "'.join(sorted(symbols)))
    f.write('''"});
''')
    f.close()
    print(f'Created/updated {fn} with {len(symbols)} symbols.')


if __name__ == '__main__':
    main()
