#!/usr/bin/env python
# coding: utf-8

# ## Importing Libraries

# In[1]:


import os
import datetime
import eyed3
import shutil
import mysql.connector


# ## Logfile creation

# In[2]:


timestamp = str(datetime.datetime.now().strftime("%Y%m%d_%H-%M-%S"))
filename = ''.join(['log_',timestamp])
log_file = open(file=filename,mode='w',encoding='utf-8')


# ## Source and Destination

# In[3]:


source = "/mnt/d/Songs/complete songs"
destination = "/mnt/d/Songs/complete songs"


# ## Utility functions

# In[4]:


def copy_file(subdir,filename,destination,count=0):
    try:
        file_path = os.path.join(subdir, files)
        shutil.copy(file_path,destination)
        log_file.writelines(['SUCCESS: '+file_path+' successfully copied\n'])
        count += 1
    except FileNotFoundError:
        log_file.writelines(['ERROR: '+file_path+' not found\n'])
    except shutil.SameFileError:
        new_filename = files[:-4]+'_copy'+files[-4:]
        new_file_path = os.path.join(subdir,new_filename)
        log_file.writelines(['ERROR: '+file_path+' already exists. Renaming to '+new_filename+'\n'])
        os.rename(file_path,new_file_path)
        copy_file(subdir,filename,destination)
        
def rename_file(source,filename, new_filename):
    try:
        file_path = os.path.join(source, filename)
        new_file_path = os.path.join(source, new_filename)
        os.rename(file_path, new_file_path)
        log_file.write('SUCCESS: '+file_path+' renamed to '+new_file_path+' ...\n')
    except FileExistsError:
        new_filename = filename[:-4]+'_copy'+filename[-4:]
        new_file_path = os.path.join(source, new_filename)
        os.rename(file_path, new_file_path)
        log_file.write('SUCCESS: '+file_path+' renamed to '+new_file_path+' ...\n')


# ## Bring files to parent directory

# In[5]:


count = 0

for subdir,inner_dir,list_files in os.walk(source):
    if subdir == "/mnt/d/Songs/complete songs":
        pass
    else:
        for files in list_files:
            copy_file(subdir,files,destination,count)
#         try:
#             os.rmdir(subdir)
#             log_file.write('Subdirectory '+subdir+' removed...\n')
#         except OSError:
#             log_file.write('The directory is not empty: '+subdir+' ...\n')
log_file.write(str(count)+' number of files successfully copied')


# ## Data Cleaning

# In[36]:


list_files = list(os.walk(source))[0][2]
for files in list_files:
    if files[-3:].lower() not in ['mp3','wma','m4a','mp4','mkv']:
        print(files)


# ## Remove digits in suffix

# In[37]:


for count,filename in enumerate(os.listdir(source)):
    
    if(filename[-3:].lower() == 'mp3'):
        log_file.write('Handling an mp3 file...\n')
        log_file.write('Filename: '+filename+'\n')
        
        
        new_filename = ''
        start = 0 

        #remove non-alphabetic prefix
        for char in filename.lower():
            if char < 'a' or char > 'z':
                start += 1
            else:
                break

        # start renaming from first letter
        for char in filename[start:]:

            if char == '.': #start of extension part - not necessarily
                break
            else:
                new_filename += char
                
        new_filename += '.mp3'
        new_filename = new_filename.lower()
        if filename.lower() == new_filename:
            log_file.write('No numeric characters found in filename, proceeding...\n')
            pass
        else:
            if len(new_filename) == 4:
                log_file.write('Only numeric characters found in filename, renaming to untitled...\n')
                new_filename = 'untitled.mp3'
            rename_file(source,filename, new_filename)
    else:
        log_file.write(filename+'  is not an mp3 file, skipping...\n')


# ## Reading music file metadata

# In[ ]:


audiofile = eyed3.load("D:\Songs\complete songs\English\Alan Walker - Tired.mp3")

## need DB to find music tags OR create AI to group music by artist, tag later OR create music


for count,filename in enumerate(os.listdir(source)):
    print(filename)


# In[ ]:


source = "D:/Songs/complete songs/"

extension_count = {}

for count,filename in enumerate(os.listdir(source)):
    extension = filename[-3:]
    if extension not in extension_count:
        extension_count[extension] = 1
    else:
        extension_count[extension] += 1

print(extension_count)


# In[ ]:


source = "/mnt/d/Songs/complete songs"

timestamp = str(datetime.datetime.now().strftime("%Y%m%d_%H-%M-%S"))
filename = ''.join(['log_',timestamp])


for count,filename in enumerate(os.listdir(source)):
    print(filename)


# In[6]:


print(count)


# In[ ]:




