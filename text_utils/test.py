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

def random_token_string():
    n_tokens = random.randint(0, 1024)
    return [random_string() for i in xrange(n_tokens)]


class CleanTokensText(ut.TestCase):

    def test_examples(self):
        self.assertEqual('the rain in spain', tu.clean_tokens('the rain in spain'))
        self.assertEqual('the ( rain ) in . spain', tu.clean_tokens('the (rain) in. spain'))
        self.assertEqual(u'新 建 总 结'.encode('utf-8'), tu.clean_tokens(u'新建总结'.encode('utf-8')))

    def test_clean_token_list(self):
        vals = ['-abc\t%s' % ' '.join(random_token_string()) for v in range(100)]
        cleaned = tu.clean_token_list(vals, 1)
        self.assertEqual(cleaned, [v.lower() for v in vals])


class HashTokensTest(ut.TestCase):

    def test_empty_string(self):
        self.assertEqual(list(tu.hash_tokens('')), [[],[]])

    def test_random_strings(self):
        for i in xrange(10):
            tokens = random_token_string()
            target_hashes = map(hash_string, tokens)
            big_string = ' '.join(tokens)
            res_hashes, res_offsets = tu.hash_tokens(big_string)
            res_hashes = list(res_hashes)
            self.assertEqual(len(target_hashes), len(res_hashes))
            for i, (a, b) in enumerate(izip(target_hashes, res_hashes)):
                self.assertEqual(a, b, msg='%d: %d != %d' % (i, a, b))

class SubstitutePropernaemsTest(ut.TestCase):

    def test_google_play(self):
        test_strings = '''
-rgb\tan application attempted to use a bad version of google play services .
-rin\tan application attempted to use a bad version of google play services .
-rla\tແ ອ ັ ບ ພ ລ ິ ເ ຄ ຊ ັ ນ ໄ ດ ້ ພ ະ ຍ າ ຍ າ ມ ໃ ຊ ້ google play services ເ ວ ີ ຊ ັ ນ ທ ີ ່ ບ ໍ ່ ສ າ ມ າ ດ ໃ ຊ ້ ໄ ດ ້ .
-rbr\tum aplicativo tentou usar uma vers ã o errada do google play services .
-pt\tum aplicativo tentou usar uma vers ã o errada do google play services .
-rgb\tan application requires google play services to be enabled .
-rin\tan application requires google play services to be enabled .
-it\t" un'applicazione richiede l'attivazione di google play services . "
-rge\tა პ ლ ი კ ა ც ი ა ს ა ჭ ი რ ო ე ბ ს გ ა ა ქ ტ ი უ რ ე ბ უ ლ google play services .
-rla\tແ ອ ັ ບ ພ ລ ິ ເ ຄ ຊ ັ ນ ຕ ້ ອ ງ ກ າ ນ ເ ປ ີ ດ ນ ຳ ໃ ຊ ້ google play services .
-rbr\tum aplicativo requer a ativa ç ã o do google play services .
-pt\tum aplicativo requer a ativa ç ã o do google play services .
-rgb\tan application requires installation of google play services .
-rin\tan application requires installation of google play services .
-it\t" un'applicazione richiede l'installazione di google play services . "
-rbr\tum aplicativo requer a instala ç ã o do google play services .
-pt\tum aplicativo requer a instala ç ã o do google play services .
'''.split('\n')
        test_strings = [v for v in test_strings if v.strip()]
        random.shuffle(test_strings)
        target_strings = [v.replace('google play services', '$$propername3$$ $$propername2$$ $$propername0$$') for v in test_strings]

        for i in range(len(test_strings)):
            x = test_strings[:i]
            result_strings = tu.substitute_propernames(x)

        for a, b in zip(result_strings, target_strings):
            self.assertEqual(a, b)

if __name__ == '__main__':
    ut.main()

