from functools import lru_cache
import json
from sys import stdin, stdout
import tempfile
import os
from urllib.parse import urlparse

from azure.storage.blob import BlockBlobService


def read():
    request = json.loads(stdin.readline().strip())
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


def report_error(code, message, event=None, oid=None):
    payload = {
        'error': {'code': code, 'message': message}
    }
    if event:
        payload['event'] = event
    if oid:
        payload['oid'] = oid
    write(payload)


def temp_file_path():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    return path


def handle_transfer(operation, transfer):
    oid = transfer['oid']
    try:
        href = transfer['action']['href']
        (account_name, container_name,
         blob_name, sas_token) = parse_href(href)
        service = block_blob_service(account_name, sas_token)

        last_current = 0
        def progress_cb(current, total):  # noqa
            report_progress(oid, current, current - last_current)  # noqa
            last_current = current  # noqa

        if operation == 'upload':
            path = transfer['path']
            service.create_blob_from_path(container_name, blob_name, path,
                                          progress_callback=progress_cb)
        elif operation == 'download':
            path = temp_file_path()
            service.get_blob_to_path(container_name, blob_name, path,
                                     progress_callback=progress_cb)

        complete_payload = {'event': 'complete', 'oid': oid}
        if operation == 'download':
            complete_payload['path'] = path
        write(complete_payload)
    except Exception as err:
        report_error(2, 'transfer failed: {}'.format(err),
                     event='complete', oid=oid)


def main():
    try:
        init = read()
        assert init['event'] == 'init'
        operation = init['operation']
        write({})
    except Exception as err:
        report_error(32, 'init failed: {}'.format(err))

    try:
        transfer = read()
        while transfer:
            if transfer['event'] == 'terminate':
                break
            handle_transfer(operation, transfer)
            transfer = read()
    except Exception as err:
        report_error(64, 'unexpected runtime error: {}'.format(err))


if __name__ == "__main__":
    main()
