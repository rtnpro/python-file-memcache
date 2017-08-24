import os
import sys
import hashlib

from pymemcache.client.base import Client


DEFAULT_MEMLIMIT = 5 * 1024 * 1025
DEFAULT_MAX_REQS_PER_CONN = 20


class FileCacheHandler:

    def __init__(self, client, max_requests=DEFAULT_MAX_REQS_PER_CONN,
                 mem_limit=DEFAULT_MEMLIMIT):
        self.client = client
        self.max_requests = max_requests
        self.mem_limit = mem_limit

    def set(self, path):
        abspath = os.path.abspath(path)
        key_prefix = self._get_key_prefix(abspath)
        chunk_size = self._get_chunk_size(abspath)

        buf = {
            'data': {},
            'len': 0,
            'size': 0
        }

        def flush():
            self.client.set_many(buf['data'])
            buf['data'], buf['len'], buf['size'] = {}, 0, 0

        with open(path, 'rb') as f:
            index = 0
            while True:
                if buf['len'] >= self.max_requests or \
                        buf['size'] + chunk_size >= self.mem_limit:
                    flush()
                s = f.read(chunk_size)
                if not s:
                    break
                buf['data'][self._get_key(key_prefix, index)] = s
                buf['len'] += 1
                buf['size'] += chunk_size
                index += 1
            flush()

        self.client.set('chunk_count_{}'.format(key_prefix), index)

    def get(self, path):
        abspath = os.path.abspath(path)
        key_prefix = self._get_key_prefix(abspath)
        chunk_count = int(
            self.client.get('chunk_count_{}'.format(key_prefix)))
        print(chunk_count)
        s = b''
        for index in range(chunk_count):
            s += self.client.get(self._get_key(key_prefix, index))
        return s

    def _get_chunk_size(self, path):
        # since the file is large, we'll choose chunk size as 900 KB
        # better way would be to use os.stat(file) to get it's size
        # and calculate optimal chunk size
        return 900 * 1024

    def _get_key_prefix(self, path):
        return hashlib.md5(path.encode()).hexdigest()[:8]

    def _get_key(self, key_prefix, index):
        return '{}_{:05d}'.format(key_prefix, index)


def set_file(path):
    cache.set(path)


def get_file(path, dest=None):
    if dest is None:
        dest = os.path.split(path)[-1]
    with open(dest, 'wb') as f:
        f.write(cache.get(path))
    print('Output file dumped at {}'.format(dest))


def echo(value):
    client.set('echo', value)
    print(client.get('echo'))


def set(key, value):
    client.set(key, value)


def get(key):
    print(client.get(key))


def set_many(items):
    items = items.strip(',')
    client.set_many(
        dict([item.split(':') for item in items.strip(',').split(",")])
    )


def get_many(keys):
    print(client.get_many(keys.strip(',').split(",")))


if __name__ == '__main__':
    client = Client((os.environ.get('MEMCACHED_HOST', '127.0.0.1'),
                     os.environ.get('MEMCACHED_PORT', 11211)))
    cache = FileCacheHandler(client)
    argc = len(sys.argv)
    if argc < 2:
        print("""Usage:
echo <msg>""")
    else:
        cmd = sys.argv[1]
        locals().get(cmd)(*sys.argv[2:])
