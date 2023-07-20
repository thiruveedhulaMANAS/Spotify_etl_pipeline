import pandas as pd
import json
import boto3
from datetime import datetime
from io import StringIO

def song_func(x):
    song_list = []
    for i in range(len(x['tracks']['items'])):
        song_info = x['tracks']['items'][i]['track']
        song_id = song_info['id']
        song_name = song_info['name']
        song_duration_ms = song_info['duration_ms']
        song_url = song_info['external_urls']['spotify']
        song_popularity = song_info['popularity']
        album_id = song_info['album']['id']
        artist_id = song_info['album']['artists'][0]['id']
        song = {'Song_id' : song_id,
            'Name': song_name,
            'Album_id' : album_id,
            'Artist_id' : artist_id,
            'Url':song_url,
            'Duration_ms' : song_duration_ms,
            'Popularity':song_popularity}
        song_list.append(song)
    return song_list

def album_func(x):
    album_list = []
    for i in range(len(x['tracks']['items'])):
        album_info = x['tracks']['items'][i]['track']['album']
        album_id = album_info['id']
        album_name = album_info['name']
        album_release_date  = album_info['release_date']
        album_total_tracks= album_info['total_tracks']
        album_url = album_info['external_urls']['spotify']
        album = {'album_id':album_id,
                'Name':album_name,
                'Release_date':album_release_date,
                'Total_tracks':album_total_tracks,
                'URL':album_url}
        album_list.append(album)
    return album_list

def artist_func(x):
    artist_list = []
    for i in range(len(x['tracks']['items'])):
        artist_info = x['tracks']['items'][i]['track']['album']['artists'][0]
        artist_id = artist_info['id']
        artist_name = artist_info['name']
        artist_url = artist_info['external_urls']['spotify']
        artist = {'artist_id':artist_id,'artist_name':artist_name,'external_url':artist_url}
        artist_list.append(artist)
    return artist_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket="manas-bucket"
    Key ='spotify/to_processing/'
    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1]=='json':
            response = s3.get_object(Bucket=Bucket,Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    for file in spotify_data:
        song = song_func(file)
        album = pd.DataFrame.from_dict(album_func(file)).drop_duplicates(subset='album_id').reset_index(drop=True)
        artist = pd.DataFrame.from_dict(artist_func(file)).drop_duplicates(subset='artist_id').reset_index(drop=True)
    
        song_df = pd.DataFrame.from_dict(song)
    
        song_filename = "outputs/song_dim/songs_transformed_" + str(datetime.now()) + ".csv"
        album_filename = "outputs/album_dim/album_transformed_" + str(datetime.now()) + ".csv"
        artist_filename  = "outputs/artist_dim/artist_transformed_" + str(datetime.now()) + ".csv"
    
        song_key = StringIO()
        song_df.to_csv(song_key,index=True,header=True)
        song_content = song_key.getvalue()
        s3.put_object(Bucket='spotify-data-1',Key=song_filename,Body=song_content)
        
        album_key = StringIO()
        album.to_csv(album_key,index=True)
        album_content = album_key.getvalue()
        s3.put_object(Bucket='spotify-data-1',Key=album_filename,Body=album_content)
        
        artist_key = StringIO()
        artist.to_csv(artist_key,index=True)
        artist_content = artist_key.getvalue()
        s3.put_object(Bucket='spotify-data-1',Key=artist_filename,Body=artist_content)

    for key in spotify_keys:
        s3.delete_object(Bucket=Bucket, Key=key)
    


