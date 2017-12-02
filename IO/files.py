import requests
import os
import zipfile


def unzipper(file_path, dirname):
    with zipfile.ZipFile(file_path) as zf:
        files = zf.namelist()
        zf.extractall(dirname)


def download_http(url, file_path):
    r = requests.get(url)
    with open(file_path, "wb") as f:
        f.write(r.content)


def download(url, file_path):
    folder = os.path.dirname(file_path)
    if not os.path.exists(folder):
        os.makedirs(folder)
    print("Starting request for file: " + file_path)
    # start http request
    if url.startswith('http://'):
        return download_http(url, file_path)
    # downlaod file form ftp
    elif url.startswith('ftp://'):
        return self.download_ftp(url, file_path)
