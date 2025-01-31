import unittest as ut
from demo import backend
import os
import numpy as np

class TestCleanStrings(ut.TestCase):

    def assertArrayEqual(self, a, b):
        self.assertEqual(np.count_nonzero(a - b), 0)

    def assertArrayClose(self, a, b):
        min_size = min(a.shape[0], b.shape[0])
        self.assertLess(min_size - np.intersect1d(a, b, assume_unique=True).shape[0], 1)

    def assertMonotonic(self, v):
        self.assertEqual(np.count_nonzero(v[1:] < v[:-1]), 0)

    def assertValidSSTable(self, v):
        size = v.offsets.shape[0]
        self.assertGreaterEqual(np.count_nonzero(v.offsets), size-1)
        self.assertMonotonic(v.offsets)

    def test_clean_hashes_keys_align(self):
        self.assertArrayEqual(backend().clean_strings.hashes, backend().clean_hashes.hashes)
        raw_hashes_from_clean = np.fromfile(backend().clean_hashes.strings)
        diff = np.setdiff1d(raw_hashes_from_clean, backend().raw_strings.hashes)
        self.assertEqual(diff.shape[0], 0)

    @ut.skip('')
    def test_translations_keys_align(self):
        self.assertArrayEqual(backend().translations.hashes, backend().clean_strings.hashes)

    @ut.skip('')
    def test_ambiguities_keys_align(self):
        self.assertArrayEqual(backend().ambiguities.hashes, backend().clean_strings.hashes)

    def test_translations_keys_align(self):
        self.assertArrayClose(backend().translations.hashes, backend().clean_strings.hashes)

    def test_ambiguities_keys_align(self):
        self.assertArrayClose(backend().ambiguities.hashes, backend().clean_strings.hashes)

    @ut.skip('')
    def test_raw_keys_align(self):
        raw_hashes = np.fromfile(backend().clean_indexes.strings, dtype=np.uint64)
        raw_hashes = np.unique(raw_hashes)
        self.assertTrue(raw_hashes == backend().raw_strings.hashes)


if __name__ == '__main__':
    ut.main()
