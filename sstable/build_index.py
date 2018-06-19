import os, sys
import sstable
sys.path.insert(0, '/'.join(os.path.abspath(__file__).split('/')))
print sstable.__file__

if __name__ == '__main__':
    idxname = sys.argv[1]
    stringsname = sys.argv[2]

    sstable.build_index(sys.stdin,
            '%s.tmp' % idxname, idxname,
            '%s.tmp' % stringsname, stringsname)

    #os.unlink('%s.tmp' % idxname)
    #os.unlink('%s.tmp' % stringsname)
