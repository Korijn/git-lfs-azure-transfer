import json
import sys

from azure.storage.blob import BlockBlobService


def read():
    request = json.loads(sys.stdin.readline().strip())
    if request['event'] == 'terminate':
        sys.exit()
    return request


def write(response):
    sys.stdout.write(json.dumps(response) + '\n')
    sys.stdout.flush()


def download(init):
    pass


def upload(init):
    pass


def main():
    init = read()
    if init['event'] == 'init':
        respond({})
        if init['operation'] == 'download':
            download(init)
        elif init['operation'] == 'upload':
            upload(init)


if __name__ == "__main__":
    main()
