import unittest
import os
import subprocess

from pymemcache.client.base import Client

from app import FileCacheHandler


class TestFileCache(unittest.TestCase):

    def setUp(self):
        client = Client(('localhost', 11211))
        self.cache = FileCacheHandler(client)

    def test_src_dest_are_equal(self):
        src_file = 'bigoldfile.dat'
        self.setup_test_file(src_file)
        self.cache.set(src_file)
        output = self.cache.get(src_file)

        with open(src_file, 'rb') as f:
            expected_content = f.read()

        self.assertEqual(expected_content, output)

    def setup_test_file(self, path):
        if not (os.path.exists(path) and os.path.isfile(path)):
            subprocess.check_call(
                'dd if=/dev/urandom of={} bs=1048576 count=250'
                .format(path))
