import requests
import os
import zipfile


def unzipper(file_path, dirname):
    with zipfile.ZipFile(file_path) as zf:
        files = zf.namelist()
        zf.extractall(dirname)


def download_http(url, file_path):
    response = requests.get(url, stream=True)
    if response.status_code == requests.codes.ok:
        # save download to given location
        with open(file_path, 'wb') as f:
            # fix that with nice parameters
            for chunk in response.iter_content(1000000):
                f.write(chunk)
                return True
            else:
                return False


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
