import os


def create_dir(dir_path):
    print('Looking for Dir Path : {}'.format(dir_path))
    if not os.path.exists(dir_path):
        print('Dir not available. Creating...')
        os.mkdir(dir_path)
    else:
        print("Dir already exists")
    print()


def remove_dir(dir_path):
    print('Looking for Dir Path : {}'.format(dir_path))
    if os.path.exists(dir_path):
        print('Dir available. Deleting...')
        os.rmdir(dir_path)
    else:
        print("Dir doesn't exists")
    print()


def delete_file(file_path):
    print('Looking for File Path : {}'.format(file_path))
    if os.path.exists(file_path):
        print('File available. Deleting...')
        os.remove(file_path)
    else:
        print("File doesn't exists")
    print()
