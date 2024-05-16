
import os

cwd = os.getcwd()

found_folders = os.listdir(cwd)

to_ = str.maketrans("-", "_")
from_ = str.maketrans("_", "-")
options = {'-': from_, '_': to_}

while True:
    choice = input('¿Hacia qué formato desea transformar las carpetas? -/_: ')
    if choice in ("-", "_"):
        use_translator = options[choice]
        break
    print('Opción incorrecta\n')


for folder_name in found_folders:
    
    if '.' in folder_name:
        continue
    try:
        os.rename(f'{cwd}\{folder_name}', f'{cwd}\{folder_name.translate(use_translator)}')
    except:
        continue