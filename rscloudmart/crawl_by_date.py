#!/usr/bin/env python3
import websocket
import threading
import datetime
import json
import pickle
from urllib import request
import pandas as pd
import os
import argparse

class Task(threading.Thread):
    def __init__(self, date, output_folder, timeout):
        super().__init__()
        self.__results = []
        self.__timeout = timeout
        self.__datestr = date.strftime('%Y-%m-%d')
        self.__output_file = os.path.join(output_folder, self.__datestr + '.csv')

    def log(self, msg):
        print('{}: {}'.format(self.__datestr, msg))

    def run(self):
        if os.path.exists(self.__output_file):
            self.log('already exists, skip this date')
            return
    
        self.ws = websocket.create_connection("ws://www.rscloudmart.com/datatest")
        self.open = True

        self.recv_cnt = 0

        loop_cnt = 0
        while self.open:
            if loop_cnt == 0:
                self.log("connecting")
            elif loop_cnt == 1:
                self.log("downloading")
            timer = threading.Timer(self.__timeout, self.__close)
            timer.start()
            try:
                msg = self.ws.recv()
                self.__on_msg(self.ws, msg)
            except:
                pass
            timer.cancel()
            loop_cnt += 1

        self.__on_close(self.ws)

    def __close(self):
        self.open = False
        self.ws.abort()
        self.ws.close()

    def __on_close(self, ws):
        if len(self.__results) > 0:
            df = pd.DataFrame(self.__results)
            df.to_csv(self.__output_file)
        self.log("closed")

    def __on_msg(self, ws, msg):
        parsed_msg = json.loads(msg)
        if 'userId' in parsed_msg:
            user_id = parsed_msg['userId']
            self.__trigger(self.__datestr, user_id)
        else:
            self.recv_cnt += len(parsed_msg['list'])
            self.log('recv {} / {}'.format(self.recv_cnt, parsed_msg['total']))
            self.__results += parsed_msg['list']

    def __trigger(self, date, user_id):
        start_url = "http://www.rscloudmart.com/dataProduct/onedaynew?dateStr={}&userId={}".format(date, user_id)
        request.urlopen(start_url)

class readable_dir(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir=values
        if not os.path.isdir(prospective_dir):
            parser.error("readable_dir: '{0}' is not a valid directory".format(prospective_dir))
        if os.access(prospective_dir, os.R_OK):
            setattr(namespace,self.dest,prospective_dir)
        else:
            parser.error("readable_dir: '{0}' is not a readable directory".format(prospective_dir))

def get_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-o', 
        '--output_folder', 
        default='.',
        help='output folder',
        action=readable_dir,
        required=True)

    parser.add_argument('-s', 
        '--start_date', 
        default=datetime.date.today().strftime('%Y-%m-%d'),
        help='start date in the format of YYYY-MM-DD')

    parser.add_argument('-e', 
        '--end_date', 
        default=datetime.date.today().strftime('%Y-%m-%d'),
        help='end date in the format of YYYY-MM-DD')

    parser.add_argument('-t', 
        '--websocket_timeout',
        default=5,
        type=float,
        help='websocket will be closed after 5 (default) seconds')

    return parser.parse_args()

def crawl(start_date, end_date, output_folder, websocket_timeout):
    print('crawling data from {} to {}'.format(start_date, end_date))

    start_date, end_date = map(lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), (start_date, end_date))

    date = start_date
    while date <= end_date:
        task = Task(date, output_folder, websocket_timeout)
        task.start()
        task.join()
        date += datetime.timedelta(days=1)

if __name__ == "__main__":
    args = get_args()
    crawl(args.start_date, args.end_date, args.output_folder, args.websocket_timeout)