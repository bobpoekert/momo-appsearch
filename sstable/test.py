import unittest as tt
import sstable
import numpy as np
import os, struct
import itertools as it
import numpy.testing as nt

TEST_ARRAY_SIZE = (4096,)

class TestCase(tt.TestCase):

    def assertNonzeroCount(self, v, c):
        self.assertEqual(np.nonzero(v)[0].shape[0], c)

    def assertSorted(self, v):
        self.assertNonzeroCount(
                v[1:] < v[:-1], 0)


class DuplicateMaskTest(TestCase):

    def test_zeros(self):
        zz = np.zeros(TEST_ARRAY_SIZE, dtype=np.uint32)
        mask = sstable.duplicate_mask(zz)
        self.assertNonzeroCount(mask, 1)

    def test_ones(self):
        zz = np.ones(TEST_ARRAY_SIZE, dtype=np.uint32)
        mask = sstable.duplicate_mask(zz)
        self.assertNonzeroCount(mask, 1)

    def test_range(self):
        vv = np.arange(TEST_ARRAY_SIZE[0], dtype=np.uint32)
        mask = sstable.duplicate_mask(vv)
        self.assertNonzeroCount(mask, TEST_ARRAY_SIZE[0])

class HashesFromFdTest(TestCase):

    def setUp(self):
        self.infile = open('test_lines.txt', 'r')
        sstable.build_index(self.infile,
                'test_hashes.bin.tmp', 'test_hashes.bin', 'test_strings.txt.tmp', 'test_strings.txt')
        self.bin = np.memmap('test_hashes.bin.tmp', dtype=np.uint32).reshape((-1, 2))
        self.hashes = self.bin[:, 0]
        self.offsets = self.bin[:, 1]
        self.bin2 = np.memmap('test_hashes.bin', dtype=np.uint32)
        self.bin2_count = self.bin2.shape[0] / 2
        self.hashes2 = self.bin2[:self.bin2_count]
        self.offsets2 = self.bin2[self.bin2_count:]
        self.table = sstable.SSTable('test_hashes.bin', 'test_strings.txt')
        self.tmp_strings = open('test_strings.txt.tmp')
        self.strings = open('test_strings.txt')

    def tearDown(self):
        self.infile.close()
        self.tmp_strings.close()
        self.strings.close()
        os.unlink('test_hashes.bin')
        os.unlink('test_hashes.bin.tmp')
        os.unlink('test_strings.txt')
        os.unlink('test_strings.txt.tmp')

    def iter_strings(self):
        with open('test_strings.txt.tmp', 'r') as inf:
            size = struct.unpack('<I', inf.read(4))[0]
            yield (size, inf.read(size))

    def bin_lengths(self):
        prev = None
        for idx in xrange(1, self.offsets.shape[0]):
            v = self.offsets[idx]
            if prev is None:
                yield v
            else:
                yield v - prev
            prev = v

    def test_lengths_match(self):
        for (ss, sv), b in it.izip(self.iter_strings(), self.bin_lengths()):
            self.assertEqual(ss + 4, b)

    def compute_offsets(self):
        offset_acc = 0
        computed_offsets = []
        with open('test_strings.txt.tmp', 'r') as inf:
            while 1:
                blob = inf.read(4)
                if len(blob) < 4:
                    break
                size = struct.unpack('<I', blob)[0]
                computed_offsets.append(offset_acc)
                offset_acc += size
                offset_acc += 4
                inf.read(size)
        return computed_offsets

    def test_hashes_match_strings(self):
        for idx, (_, row) in enumerate(self.iter_strings()):
            self.assertEqual(self.hashes[idx], sstable.hash_string(row))

    def get_string_length(self, offset):
        self.tmp_strings.seek(offset)
        return struct.unpack('<I', self.tmp_strings.read(4))[0]

    def get_res_string(self, offset, length):
        self.strings.seek(offset)
        return self.strings.read(length)

    def test_valid_tmp_offsets(self):
        lengths = [self.get_string_length(v) for v in self.compute_offsets()]
        self.assertLess(max(lengths), 100000)

    def test_offsets(self):
        nt.assert_array_equal(self.compute_offsets(), self.offsets)

    def test_sorted_offsets(self):
        sorted_hashes, sorted_offsets = sstable.sort_uniqify_hashes(self.hashes, self.offsets)
        self.assertSorted(sorted_hashes)
        target_offsets = set(self.compute_offsets())
        for offset in sorted_offsets:
            self.assertIn(offset, target_offsets)

    def test_valid_sorted_lengths(self):
        sorted_hashes, sorted_offsets = sstable.sort_uniqify_hashes(self.hashes, self.offsets)
        for idx, offset in enumerate(sorted_offsets):
            self.assertLess(self.get_string_length(offset), 100000)

    def test_hashes_are_sorted(self):
        self.assertSorted(self.hashes2)

    def test_offsets_are_nonzero(self):
        self.assertGreater(np.nonzero(self.offsets2)[0].shape[0], 0)

    def test_hashes_are_nonzero(self):
        self.assertGreater(np.nonzero(self.hashes2)[0].shape[0], 0)

    def test_lookups(self):
        for (_, row) in self.iter_strings():
            hh = sstable.hash_string(row)
            idx = np.searchsorted(self.hashes2, hh)
            self.assertEqual(hh, self.hashes2[idx])
            offset = self.offsets2[idx]
            length = self.offsets2[idx + 1] - offset
            string = self.get_res_string(offset, length)
            self.assertEqual(string, row)

if __name__ == '__main__':
    tt.main()
