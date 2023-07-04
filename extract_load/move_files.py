import os
import shutil

# extracted files folder
extracted_folder = 'extracted_files'

# csv folder
csv_folder = 'csv_files'

if not os.path.exists(csv_folder):
    os.makedirs(csv_folder)

# iterate over unziped files
for root, dirs, files in os.walk(extracted_folder):
    for file_name in files:
        if file_name.endswith('.csv'):
            current_file_path = os.path.join(root, file_name)
            new_file_path = os.path.join(csv_folder, file_name)
            # move files to csv folder
            shutil.move(current_file_path, new_file_path)
            

