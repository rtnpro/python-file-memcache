import os
import sys

from pymemcache.client.base import Client


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
    argc = len(sys.argv)
    if argc < 2:
        print("""Usage:
echo <msg>""")
    else:
        cmd = sys.argv[1]
        locals().get(cmd)(*sys.argv[2:])
