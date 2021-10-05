# ## Step 1: Bring all files from subdirectories to main directory

# ### Utility Function

# In[5]:

import os
import shutil


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


def bring_files_to_parent_dir():
    print("Script successfully ran")
    pass
    
bring_files_to_parent_dir()
