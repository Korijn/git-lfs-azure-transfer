from functools import lru_cache
import json
from sys import stdin, stdout
import tempfile
import os
from urllib.parse import urlparse

from azure.storage.blob import BlockBlobService


def read():
    request = json.loads(stdin.readline().strip())
    if request['event'] == 'terminate':
        return
    return request


def write(response):
    stdout.write(json.dumps(response) + '\n')
    stdout.flush()


@lru_cache(maxsize=None)
def block_blob_service(account_name, sas_token):
    return BlockBlobService(account_name=account_name, sas_token=sas_token)


def parse_href(href):
    url = urlparse(href)
    account_name = url.hostname.split('.')[0]
    _, container_name, blob_name = url.path.split('/')
    sas_token = url.query
    return account_name, container_name, blob_name, sas_token


def report_progress(oid, bytes_so_far, bytes_since_last):
    write({
        'event': 'progress',
        'oid': oid,
        'bytesSoFar': bytes_so_far,
        'bytesSinceLast': bytes_since_last,
    })


def temp_file_path():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    return path


def handle_transfers(operation):
    transfer = read()
    while transfer:
        oid, href = transfer['oid'], transfer['action']['href']
        account_name, container_name, blob_name, sas_token = parse_href(href)
        service = block_blob_service(account_name, sas_token)

        last_current = 0
        def progress_callback(current, total):  # noqa
            report_progress(oid, current, current - last_current)  # noqa
            last_current = current  # noqa

        if operation == 'upload':
            path = transfer['path']
            service.create_blob_from_path(container_name, blob_name, path,
                                          progress_callback=progress_callback)
        elif operation == 'download':
            path = temp_file_path()
            service.get_blob_to_path(container_name, blob_name, path,
                                     progress_callback=progress_callback)

        complete_payload = {'event': 'complete', 'oid': oid}
        if operation == 'download':
            complete_payload['path'] = path
        write(complete_payload)

        transfer = read()


def main():
    init = read()
    assert init['event'] == 'init'
    write({})
    handle_transfers(init['operation'])


if __name__ == "__main__":
    main()
