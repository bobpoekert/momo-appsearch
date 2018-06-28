import unittest as ut
import subprocess as sp
import numpy as np
from tempfile import NamedTemporaryFile

def get_test_outp():
    with NamedTemporaryFile() as temp_bin:
        test_lines = sp.Popen(['tail', '-n', '100000', '/mnt/zapk/clean.txt'], stdout=sp.PIPE)
        coo = sp.Popen(['./cooccurrence', temp_bin.name], stdin=test_lines.stdout)
        coo.wait()

        return np.fromfile(temp_bin, dtype=np.uint64).reshape((-1, 3))

def is_sorted(arr):
    return np.count_nonzero(arr[1:] < arr[:-1]) == 0


class SortedTest(ut.TestCase):

    def test_results_sorted(self):
        mat = get_test_outp()
        self.assertTrue(is_sorted(mat[:, 0]))


if __name__ == '__main__':
    ut.main()
