import clean_text

from multiprocessing import cpu_count
import os, sys, shutil
import traceback

def split_points(infname, n_splits):
    inf_size = os.stat(infname).st_size
    split_size = inf_size / n_splits
    splits = range(0, inf_size, split_size)

    with open(infname, 'r') as inf:
        for idx in range(n_splits):
            split = splits[idx]
            print split
            inf.seek(split)
            while inf.read(1) != '\n':
                split += 1
            prev_k = None
            while 1:
                k, v = inf.readline().split('\t', 1)
                if not (prev_k is None or k != prev_k):
                    break
                split += len(k)
                split += 1
                split += len(v)
                prev_k = k
            splits[idx] = split

    return splits

def consolidate_files(basename, infnames):
    with open(basename, 'w') as boutf:
        for f2 in infnames:
            with open(f2, 'r') as ff2:
                shutil.copyfileobj(ff2, boutf)
            os.unlink(f2)

if __name__ == '__main__':
    bin_basename = sys.argv[3]
    infname = sys.argv[4]
    outfname = sys.argv[5]
    english_basename = sys.argv[6]
    groups_basename = sys.argv[7]

    pids = []
    splits = split_points(infname, cpu_count())
    outfnames = ['%s.%d' % (outfname, idx) for idx in range(len(splits))]
    bin_outfnames = ['%s.%d' % (bin_basename, idx) for idx in range(len(splits))]
    english_outfnames = ['%s.%d' % (english_basename, idx) for idx in range(len(splits))]
    groups_outfnames = ['%s.%d' % (groups_basename, idx) for idx in range(len(splits))]

    for idx in range(len(splits)):
        pid = os.fork()
        if pid == 0:
            try:
                with open(infname, 'r') as inf:
                    inf.seek(splits[idx])
                    with open(outfnames[idx], 'w') as outf:
                        with open(bin_outfnames[idx], 'w') as boutf:
                            with open(english_outfnames[idx], 'w') as eoutf:
                                with open(groups_outfnames[idx], 'w') as goutf:
                                    clean_text.run(inf, outf, boutf, eoutf, goutf,
                                            splits[idx + 1] - splits[idx] if idx < len(splits) - 1 \
                                                    else os.stat(infname).st_size - splits[idx])
            except:
                traceback.print_exc()
            finally:
                sys.exit()
        else:
            print pid
            pids.append(pid)

    while pids:
        os.waitpid(pids.pop(), 0)

    consolidate_files(outfname, outfnames)
    consolidate_files(bin_basename, bin_outfnames)
    consolidate_files(english_basename, english_outfnames)
    consolidate_files(groups_basename, groups_outfnames)
