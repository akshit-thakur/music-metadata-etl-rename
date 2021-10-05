import os

def data_cleaning(source):
    source = kwargs.get("source")
    print(list(os.walk(source)))
    list_files = list(os.walk(source))[0][2]
    for files in list_files:
        if files[-3:].lower() not in ['mp3','wma','m4a','mp4','mkv']:
            print(files)
            
data_cleaning(source="/mnt/d/Songs/complete songs") 
