import sys, traceback

sys.path.append('../sstable')
import sstable

sys.path.append('../text_utils')
import text_utils as tt

if __name__ == '__main__':

    with open(sys.argv[3], 'w') as outf:
        for idx, v in enumerate(sstable.SSTable(sys.argv[1], sys.argv[2]).raw_itervalues()):
            if v[0] == '-' and not v.startswith('-en'):
                continue
            v = v.split('\t')[1]
            v = tt.clean_tokens(v)
            if len(v) < 1:
                continue
            outf.write(v)
            outf.write('\n')
