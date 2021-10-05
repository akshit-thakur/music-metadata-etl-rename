#!/usr/bin/env python
# coding: utf-8

# ## Importing Libraries

# In[1]:


try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime
    import os
    import datetime
    import eyed3
    import shutil
    import mysql.connector
    print("All DAG modules are ok ......")
except Exception as e:
    print(f'{e}')


# ## Step 1: Bring all files from subdirectories to main directory

# ### Utility Function

# In[5]:


def copy_file(subdir,filename,destination):
    try:
        file_path = os.path.join(subdir, filename)
        shutil.copy(file_path,destination)
        print('SUCCESS: '+file_path+' successfully copied\n')
        count += 1
    except FileNotFoundError:
        log_file.writelines(['ERROR: '+file_path+' not found\n'])
    except shutil.SameFileError:
        new_filename = files[:-4]+'_copy'+files[-4:]
        new_file_path = os.path.join(subdir,new_filename)
        print('ERROR: '+file_path+' already exists. Renaming to '+new_filename)
        os.rename(file_path,new_file_path)
        copy_file(subdir,filename,destination)


# ### Main Function

# In[6]:


def bring_files_to_parent_dir(*args,**kwargs):
    pass
#     source = kwargs.get("source")
#     destination = kwargs.get('destination')
#     for subdir,inner_dir,list_files in os.walk(source):
#         if subdir == "/mnt/d/Songs/complete songs":
#             pass
#         else:
#             for files in list_files:
#                 copy_file(subdir,files,destination)


# ## Step 1.1: Data Cleaning - Remove non audio files from parent directory

# In[7]:


def data_cleaning(*args,**kwargs):
    source = kwargs.get("source")
    print(list(os.walk(source)))
    list_files = list(os.walk(source))[0][2]
    for files in list_files:
        if files[-3:].lower() not in ['mp3','wma','m4a','mp4','mkv']:
            print(files)


# ## Step 2: Removing digits in suffix

# ### Utility Function

# In[12]:


def rename_file(source,filename, new_filename):
    file_path = os.path.join(source, filename)
    new_file_path = os.path.join(source, new_filename)
    while os.path.exists(new_file_path):
        print(new_file_path+' exists')
        new_filename = new_filename[:-4]+'_copy'+filename[-4:]
        new_file_path = os.path.join(source, new_filename)
        
    os.rename(file_path, new_file_path)
    print('SUCCESS: '+file_path+' renamed to '+new_file_path+' ...')


# ### Main Function

# In[8]:


def remove_numbers(*args,**kwargs):
    source = kwargs.get("source")
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

            new_filename = filename[start:].lower()

            if filename.lower() == new_filename:
                print('No numeric characters found in filename, proceeding...')
                pass
            else:
                if len(new_filename) == 4:
                    print('Only numeric characters found in filename, renaming to untitled...')
                    new_filename = 'untitled.mp3'
                rename_file(source,filename, new_filename)
        else:
            print(filename+'  is not an mp3 file, skipping...')


# ## Step 3: Reading Music Metadata into file
# 
# Can also fire an insert statement after each record is created. But this is better way so that there is an intermediate before data load operations

# In[9]:


def write_metadata_to_file(*args,**kwargs):
    timestamp = str(datetime.datetime.now().strftime("%Y%m%d_%H-%M-%S"))
    filename = ''.join(['music_metadata_',timestamp])
    music_metadata_file = open(file=filename,mode='w',encoding='utf-8')

    source = kwargs.get("source")
    
    list_files = list(os.walk(source))[0][2]
    
    headers = ['filename','album','artist','composer','genre','get_best_date','release_date','tagging_date','title','bitrate','runtime_seconds']
    
    
    header_line = '\t'.join([header for header in headers])
    
    music_metadata_file.write(header_line+'\n')
    
    
    for filename in list_files:
        song_info = {}
            
        audiofile = eyed3.load(''.join([source,'/',filename]))
        
        song_info['filename'] = filename
        
        try:
            audiotag = audiofile.tag
            if audiotag is not None:
                song_info['album'] = audiotag.album
                song_info['artist'] = audiotag.artist
                song_info['composer'] = audiotag.composer
                song_info['genre'] = audiotag.genre
                song_info['get_best_date'] = audiotag.getBestDate()
                song_info['release_date'] = audiotag.release_date
                song_info['tagging_date'] = audiotag.tagging_date
                song_info['title'] = audiotag.title
            
        except AttributeError:
            pass #do nothing because take care of in dictionary initialization

        try:
            audio_info = audiofile.info
            if audio_info is not None:
                song_info['bitrate'] = audio_info.bit_rate_str
                song_info['runtime_seconds'] = audio_info.time_secs
        except AttributeError:
            pass


        for header in headers:
            try:
                music_metadata_file.write(str(song_info[header])+'\t')
            except:
                music_metadata_file.write('\t')
                
        music_metadata_file.write('\n')
        
    music_metadata_file.close()


# ## Step 4: Loading from file into MySQL

# In[10]:


def write_metadata_to_db(*args,**kwargs):
    #read each record, apply QC checks, trim data [no unnecessary characters] & load into DB. Other examples are rouding off runtime & removing ~ from ~160kBps
    pass


# ## Step 5: Calling DAG

# In[11]:


with DAG(
    dag_id="Music_Renaming_DAG",
    description='DAG to fix the music metadata through SQL load and search',
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime.datetime(2021, 1, 1),
    },
    catchup=False
) as f:
    
    bring_files_to_parent_dir = PythonOperator(
    task_id='bring_files_to_parent_dir',
    python_callable=bring_files_to_parent_dir,
    op_kwargs={"source":"/mnt/d/Songs/complete songs","destination":"/mnt/d/Songs/complete songs"})
    
    
    remove_numbers = PythonOperator(
    task_id='remove_numbers',
    python_callable=remove_numbers,
    op_kwargs={"source":"/mnt/d/Songs/complete songs"})
    
    data_cleaning = PythonOperator(
    task_id='data_cleaning',
    python_callable=data_cleaning,
    op_kwargs={"source":"/mnt/d/Songs/complete songs"})
    
    write_metadata_to_file = PythonOperator(
    task_id='write_metadata_to_file',
    python_callable=write_metadata_to_file,
    op_kwargs={"source":"/mnt/d/Songs/complete songs"})
    
    write_metadata_to_db = PythonOperator(
    task_id='write_metadata_to_db',
    python_callable=write_metadata_to_db,
    op_kwargs={"source":"/mnt/d/Songs/complete songs"})
    
    bring_files_to_parent_dir >> data_cleaning >> remove_numbers >> write_metadata_to_file >> write_metadata_to_db

