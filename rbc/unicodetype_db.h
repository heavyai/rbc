#include <stdint.h>
#include "path/to/numba/_unicodetype_db.h"

// To generate unicodetype_db.ll, fix the include above and compile it with
//      clang -S -emit-llvm -O2 unicodetype_db.h -o unicodetype_db.ll


/* This function is a modified copy of the private function gettyperecord from
 * CPython's Objects/unicodectype.c
 *
 * See:https://github.com/python/cpython/blob/1d4b6ba19466aba0eb91c4ba01ba509acf18c723/Objects/unicodectype.c#L45-L59
 */
void
numba_gettyperecord(Py_UCS4 code, int *upper, int *lower, int *title,
                    unsigned char *decimal, unsigned char *digit,
                    unsigned short *flags)
{
    int index;
    const numba_PyUnicode_TypeRecord *rec;

    if (code >= 0x110000)
        index = 0;
    else
    {
        index = index1[(code >> SHIFT)];
        index = index2[(index << SHIFT) + (code & ((1 << SHIFT) - 1))];
    }

    rec = &numba_PyUnicode_TypeRecords[index];
    *upper = rec->upper;
    *lower = rec->lower;
    *title = rec->title;
    *decimal = rec->decimal;
    *digit = rec->digit;
    *flags = rec->flags;
}

/* This function provides a consistent access point for the
 * _PyUnicode_ExtendedCase array defined in CPython's Objects/unicodectype.c
 * and now also as numba_PyUnicode_ExtendedCase in Numba's _unicodetype_db.h
 */
Py_UCS4
numba_get_PyUnicode_ExtendedCase(int code)
{
    return numba_PyUnicode_ExtendedCase[code];
}