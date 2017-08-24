import unittest
import os
import subprocess

import app


class TestFileCache(unittest.TestCase):

    def setUp(self):
        os.environ['MEMCACHED_HOST'] = 'localhost'
        os.environ['MEMCACHED_PORT'] = '11211'
        app.load()

    def test_src_dest_are_equal(self):
        src_file = 'test.dat'
        dest_file = 'test.dat.cached'
        self.setup_test_file(src_file)
        app.set_file(src_file)
        app.get_file(src_file, dest_file)

        src_checksum = subprocess.check_output(
            'md5sum {} | cut -d" " -f1'.format(src_file), shell=True)

        dest_checksum = subprocess.check_output(
            'md5sum {} | cut -d" " -f1'.format(dest_file), shell=True)

        self.assertEqual(dest_checksum, src_checksum)

        # test specific teardown
        os.remove(src_file)
        os.remove(dest_file)

    def setup_test_file(self, path):
        if not (os.path.exists(path) and os.path.isfile(path)):
            subprocess.check_call(
                'dd if=/dev/urandom of={} bs=1048576 count=50'
                .format(path), shell=True)
