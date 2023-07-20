import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import boto3
import os 
import json
from datetime import datetime

def lambda_handler(event, context):
    client_id_key = os.environ.get('client_id')
    client_secret_key = os.environ.get('client_secret')
    credentials = SpotifyClientCredentials(client_id=client_id_key,client_secret=client_secret_key)
    client = spotipy.client.Spotify(client_credentials_manager= credentials)
    url = 'https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=e2495315788b4da0'
    id = url.split('/')[-1].split('?')[0]
    x = client.playlist(id)
    
    filename = "spotify_raw_"+str(datetime.now())+'.json'
    
    s3 = boto3.client('s3')
    s3.put_object(Bucket="manas-bucket",Key='spotify/to_processing/'+filename,Body=json.dumps(x))