import os

def rename_file(source,filename, new_filename):
    file_path = os.path.join(source, filename)
    new_file_path = os.path.join(source, new_filename)
    while os.path.exists(new_file_path):
        print(new_file_path+' exists')
        new_filename = new_filename[:-4]+'_copy'+filename[-4:]
        new_file_path = os.path.join(source, new_filename)
        
    os.rename(file_path, new_file_path)
    print('SUCCESS: '+file_path+' renamed to '+new_file_path+' ...')
    
    
    
def remove_numbers(source):
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

remove_numberS(source="/mnt/d/Songs/complete songs")            
