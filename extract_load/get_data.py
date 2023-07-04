import os
import urllib.request
import xml.etree.ElementTree as ET

# destiny folder
dest_folder = 'zip_files'

if not os.path.exists(dest_folder):
    os.makedirs(dest_folder)

# download file from url function
def download_file(url, file_path):
    urllib.request.urlretrieve(url, file_path)

xml_url = 'https://s3.amazonaws.com/tripdata'

# get XML doc content
response = urllib.request.urlopen(xml_url)
xml_content = response.read()
tree = ET.ElementTree(ET.fromstring(xml_content))

# extract XML URLs into a list
url_list = []
root = tree.getroot()
for contents in root.iter('{http://s3.amazonaws.com/doc/2006-03-01/}Contents'):
    key = contents.find('{http://s3.amazonaws.com/doc/2006-03-01/}Key').text
    url = xml_url + '/' + key
    url = url.replace(" ", "%20")
    url_list.append(url)

# iterate over URLs and download files
for url in url_list:
    file_name = url.split('/')[-1]
    file_path = os.path.join(dest_folder, file_name)
    download_file(url, file_path)
    
