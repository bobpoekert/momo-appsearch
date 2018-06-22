#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import unittest as ut
import text_utils as tu

import sys
sys.path.append('../sstable')
from sstable import hash_string

import random
from itertools import izip

#characters = 'qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM[]\{}|;\':",./<>?`1234567890-=~!@#$%^&*()_+'
characters = 'qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM'

def random_string(max_length=128):
    length = random.randint(1, max_length)
    return ''.join([
        random.choice(characters) for i in xrange(length)])

class CleanTokensText(ut.TestCase):

    def test_examples(self):
        self.assertEqual('the rain in spain', tu.clean_tokens('the rain in spain'))
        self.assertEqual('the ( rain ) in . spain', tu.clean_tokens('the (rain) in. spain'))
        self.assertEqual(u'新 建 总 结'.encode('utf-8'), tu.clean_tokens(u'新建总结'.encode('utf-8')))

class HashTokensTest(ut.TestCase):

    def test_empty_string(self):
        self.assertEqual(list(tu.hash_tokens('')), [])

    def test_random_strings(self):
        for i in xrange(100):
            n_tokens = random.randint(0, 1024)
            tokens = [random_string() for i in xrange(n_tokens)]
            target_hashes = map(hash_string, tokens)
            big_string = ' '.join(tokens)
            res_hashes = list(tu.hash_tokens(big_string))
            self.assertEqual(len(target_hashes), len(res_hashes))
            for i, (a, b) in enumerate(izip(target_hashes, res_hashes)):
                self.assertEqual(a, b, msg='%d: %d != %d' % (i, a, b))


if __name__ == '__main__':
    ut.main()

