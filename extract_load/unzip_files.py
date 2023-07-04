import os
import zipfile

# destiny folder
extracted_folder = 'extracted_files'

if not os.path.exists(extracted_folder):
    os.makedirs(extracted_folder)

# extract ZIP file content function
def extract_zip(file_path, extract_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

zip_folder = 'zip_files'

# iterate over ZIP files in the folder
for file_name in os.listdir(zip_folder):
    if file_name.endswith('.zip'):
        zip_file_path = os.path.join(zip_folder, file_name)
        extract_subfolder = os.path.splitext(file_name)[0]
        extract_path = os.path.join(extracted_folder, extract_subfolder)
        extract_zip(zip_file_path, extract_path)
        
  
