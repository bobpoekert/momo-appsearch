import sys, traceback

sys.path.append('../sstable')
import sstable

sys.path.append('../text_utils')
import text_utils as tt

import unicodedata as ud

latin_letters= {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr)
           for uchr in unistr
           if uchr.isalpha()) # isalpha suggested by John Machin

if __name__ == '__main__':

    with open(sys.argv[3], 'w') as outf:
        for idx, v in enumerate(sstable.SSTable(sys.argv[1], sys.argv[2]).raw_itervalues()):
            if len(v) < 1:
                continue
            if v[0] == '-' and not v.startswith('-en'):
                continue
            try:
                v = v.split('\t', 1)[1]
            except:
                continue
            if not only_roman_chars(v.decode('utf-8')):
                continue
            outf.write(v)
            outf.write('\n')
