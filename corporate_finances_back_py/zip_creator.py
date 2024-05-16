import os
import zipfile


zip_output_folder = '\\'.join(os.getcwd().split('\\')[:-1]) + '\\compressed_files'

#os.mkdir(zip_output_folder)

def make_zipfile(master_folder_name, compressing_folder_name):
    with zipfile.ZipFile(f'{zip_output_folder}\\{compressing_folder_name}.zip', 'w') as myzip:
        files_in_folder = [file for file in os.listdir(f'{master_folder_name}\\{compressing_folder_name}') if '__pycache__' not in file]
        for file in files_in_folder:
            myzip.write(f'{master_folder_name}\\{compressing_folder_name}\\{file}', file)


current_folder = os.getcwd()
folders_in_current_folder = [folder_name for folder_name in os.listdir(current_folder) if ('.' not in folder_name)]
print(f'Carpetas encontradas en {current_folder}:\n{folders_in_current_folder}')


for folder in folders_in_current_folder:
    make_zipfile(current_folder, folder)
