#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia EECS E6893 Big Data Analytics
"""
This module is used to pull data from twitter API and send data to
Spark Streaming process using socket. It acts like a client of
twitter API and a server of spark streaming. It open a listening TCP
server socket, and listen to any connection from TCP client. After
a connection established, it send streaming data to it.


Usage:
  If used with dataproc:
    gcloud dataproc jobs submit pyspark --cluster <Cluster Name> twitterHTTPClient.py

  Make sure that you run this module before you run spark streaming process.
  Please remember stop the job on dataproc if you no longer want to stream data.

Todo:
  1. change the credentials to your own

"""

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import csv
import re
import os

# credentials
# TODO: replace with your own credentials
ACCESS_TOKEN = '1246119878383239168-d4SseBzvP0vcCbGETG96f72XEAfK1h'     # your access token
ACCESS_SECRET = 'J5vQnlRRNCIjKh4as1mMwm85TUGtIofwVTsuDZdtJVJsL'    # your access token secret
CONSUMER_KEY = 'y8lZ0waphFIClMKRt9Kb5uNgn'     # your API key
CONSUMER_SECRET = '80BhZmk3qBCjAFLLCmM5FH3G1JTaaC7qsN8vDS2yPouvatwIzZ'  # your API secret key

# the tags to track
tags = ['#', 'bigdata', 'spark', 'ai', 'movie']

class TweetsListener(StreamListener):
    """
    tweets listener object
    """
    def __init__(self, csocket):
        self.client_socket = csocket
    def on_data(self, data):
        try:
            msg = json.loads( data )            
            print('TEXT:{}\n'.format(msg['text']))
            sentence = re.sub("[^\u0000-\u05C0\u2100-\u214F]+", ' ', msg['text'])
            sentence = re.sub(r"http\S+", ' ', sentence)
            with open('OutputStreaming.csv', 'a', encoding='UTF-8') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow([sentence])
            self.client_socket.send( msg['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            return False
        # return True

    def on_error(self, status):
        print(status)
        return False

def sendData(c_socket, tags):
    """
    send data to socket
    """
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=tags,languages=['en'])


class twitter_client:
    def __init__(self, TCP_IP, TCP_PORT):
        self.s = s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((TCP_IP, TCP_PORT))

    def run_client(self, tags):
        try:
            self.s.listen(1)
            while True:
                print("Waiting for TCP connection...")
                conn, addr = self.s.accept()
                print("Connected... Starting getting tweets.")
                sendData(conn,tags)
                conn.close()
        except KeyboardInterrupt:
            exit


if __name__ == '__main__':
    if os.path.exists('OutputStreaming.csv'):
        os.remove('OutputStreaming.csv')
    with open('OutputStreaming.csv','a') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["sentence"])
    client = twitter_client("localhost", 9001)
    client.run_client(tags)