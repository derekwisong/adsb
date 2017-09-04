import zipfile
import logging
import json

log = logging.getLogger(__name__)

def extract_data(zipped_data, filename):
    f = zipped_data.read(filename).decode('utf-8')
    return f

def parse_data(zip_filepath, filename):
    with zipfile.ZipFile(zip_filepath) as zipped_data:
        data = json.loads(extract_data(zipped_data, filename))
        return data

def get_file_list(zip_filepath):
    with zipfile.ZipFile(zip_filepath) as z:
        return z.namelist()