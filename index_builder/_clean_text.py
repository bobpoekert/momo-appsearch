import clean_text

from multiprocessing import cpu_count
import os, sys, shutil

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

if __name__ == '__main__':
    bin_basename = sys.argv[3]
    infname = sys.argv[4]
    outfname = sys.argv[5]

    pids = []
    splits = split_points(infname, cpu_count())
    outfnames = ['%s.%d' % (outfname, idx) for idx in range(len(splits))]
    bin_outfnames = ['%s.%d' % (bin_basename, idx) for idx in range(len(splits))]

    for idx in range(len(splits)):
        pid = os.fork()
        if pid == 0:
            try:
                with open(infname, 'r') as inf:
                    inf.seek(splits[idx])
                    with open(outfnames[idx], 'w') as outf:
                        with open(bin_outfnames[idx], 'w') as boutf:
                            clean_text.run(inf, outf, boutf,
                                    splits[idx + 1] - splits[idx] if idx < len(splits) - 1 \
                                            else os.stat(infname).st_size - splits[idx])
            finally:
                sys.exit()
        else:
            print pid
            pids.append(pid)

    while pids:
        os.waitpid(pids.pop(), 0)

    with open(outfname, 'w') as outf:
        for f2 in outfnames:
            with open(f2, 'r') as ff2:
                shutil.copyfileobj(ff2, outf)
            os.unlink(f2)

    with open(bin_basename, 'w') as boutf:
        for f2 in bin_outfnames:
            with open(f2, 'r') as ff2:
                shutil.copyfileobj(ff2, boutf)
            os.unlink(f2)
