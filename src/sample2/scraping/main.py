from datetime import datetime, year, month, day
import os
import re
import zipfile

import requests
from requests_html import HTMLSession

from google.cloud import storage
from jpholiday import is_holiday, is_holiday_name

from lib.logging import write_entry

TARGET_URL    = os.environ['TARGET_URL']
UPLOAD_BUCKET = os.environ['UPLOAD_BUCKET']
ZIP_FILE_NAME = 'diff_csv_unicode_{}_' + datetime.now().strftime("%Y%m%d_%H%M%S" + '.zip') # insert file num to {} when download files

# requests.post parameters defaul config
payload = {
    'event': 'download',
    'selDlFileNo': 0
}

def main():
    try:
        today = datetime.date(year, month, day)

        if is_holiday(today):
            # output logging cloud console
            write_entry("It's {} holiday! - scraping-nta-diff-data function".format(is_holiday_name(today))) 

        else:
            rows     = scrape_target_tag()
            files_num = scrape_files_num(rows)
            
            # TODO: get files upload date Checking if the file is already gotten

            zip_file = download_files(files_num)

            storage_client = storage.Client()
            bucket         = storage_client.bucket('TARGET_BUCKET')
            blob           = bucket.blob('diff_{}.zip'.format(datetime.now().strftime("%Y%m%d")))

            # TODO: confirm target bucket if there is same name file with trying to upload file name

            blob.upload_from_filename(zip_file)
    
            os.remove(zip_file)

            # TODO: confirm target bucket if there is that updated file whether or not.

    except Exception as e:
        return print("Exception: {}. Please contact techDev0".format(e))

def scrape_target_tag():
    session  = HTMLSession()
    response = session.get(TARGET_URL)

    response.html.render(wait=30)

    return response.html.find('h2#csv-unicode + div.tbl03 table td:first-child a')

def scrape_files_num(rows):
    files_num = []

    for row in rows:
        _row: str = row.html
        file_num   = strip_file_num(_row)

        if file_num:
            files_num.append(file_num)
        else:
            print('NoneFileNumException...')

    return files_num

def strip_file_num(row):
    """
        <a href="#" onclick="return doDownload(99999);">zip 999KB</a>
        target number is...                    ^^^^^ here!
    """
    match_obj = re.search('doDownload\([0-9]+\)', row)
    file_num   = re.search('[0-9]+', match_obj.group())

    return file_num.group() # TODO: Consider whether to raise an exception

def download_files(files_num):
    for num in files_num:
        payload['selDlFileNo'] = int(num)

        r = requests.post(TARGET_URL, data=payload)

        zip_file_name = ZIP_FILE_NAME.format(num)
        with open(DOWNLOAD_PATH + zip_file_name, 'wb') as f:
            f.write(r.content)
            f.flush()

        # TODO: change Returning zip file. Do not unzip.
        unzip(zip_file_name)

def unzip(zip_file_name):
    with zipfile.ZipFile(DOWNLOAD_PATH + zip_file_name, 'w') as diff_zip:
        diff_zip.extractall(CSV_FILE_PATH) # TODO: change extractall to extract method when upload date could get
