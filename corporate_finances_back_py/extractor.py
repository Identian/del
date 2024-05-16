import zipfile
import os

this_path = os.getcwd()
lambdas_zip_list = [item for item in os.listdir() if item.endswith('.zip')]
print(f'Loz zips encontrados son: {len(lambdas_zip_list)}\n{lambdas_zip_list}')
print(this_path)



def extractor(zip_name):
    full_path = f'{this_path}\{zip_name}'
    print(f'extrayendo: {full_path}')
    ZipFile = zipfile.ZipFile(full_path)
    #extracted_folder_name = '-'.join(zip_name.replace('.zip','').split('-')[3:]) #esto es cuando el nombre de la lambda incluye lbd dev finanzas
    extracted_folder_name = zip_name.replace('.zip','')
    ZipFile.extractall(path = f"{this_path}\extracted\\{extracted_folder_name}")

for zip_file_name in lambdas_zip_list:
    extractor(zip_file_name)