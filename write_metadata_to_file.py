import os
import eyed3

def write_metadata_to_file():
    filename = 'music_metadata'
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
    
write_metadata_to_file(source="/mnt/d/Songs/complete songs")
