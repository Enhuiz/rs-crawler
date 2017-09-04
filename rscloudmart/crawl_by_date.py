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
    def __init__(self, datestr, output_file):
        super().__init__()
        self.__results = []
        self.__datestr = datestr
        self.__output_file = output_file

    def log(self, msg):
        print('{}: {}'.format(self.__datestr, msg))

    def run(self):
        self.ws = websocket.create_connection("ws://www.rscloudmart.com/datatest")
        self.open = True

        self.log('Connecting')
        msg = self.ws.recv()
        self.__on_msg(self.ws, msg)

        self.log('Downloading')
        while self.open:
            timer = threading.Timer(7, self.__close)
            timer.start()
            try:
                msg = self.ws.recv()
                self.__on_msg(self.ws, msg)
            except:
                self.__on_close(self.ws)
            timer.cancel()

    def __close(self):
        self.open = False
        self.ws.abort()
        self.ws.close()

    def __on_close(self, ws):
        df = pd.DataFrame(self.__results)
        df.to_csv(self.__output_file)
        self.log("closed")

    def __on_msg(self, ws, msg):
        parsed_msg = json.loads(msg)
        if 'userId' in parsed_msg:
            user_id = parsed_msg['userId']
            self.__trigger(self.__datestr, user_id)
        else:
            self.log('recv {} records'.format(len(parsed_msg['list'])))
            self.__results += parsed_msg['list']

    def __trigger(self, date, user_id):
        start_url = "http://www.rscloudmart.com/dataProduct/onedaynew?dateStr={}&userId={}".format(date, user_id)
        request.urlopen(start_url)

def get_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-o', 
        '--output_folder', 
        default='.',
        help='output folder',
        required=True)

    parser.add_argument('-s', 
        '--start_date', 
        default = datetime.date.today().strftime('%Y-%m-%d'),
        help='start date in the format of YYYY-MM-DD')

    parser.add_argument('-e', 
        '--end_date', 
        default = datetime.date.today().strftime('%Y-%m-%d'),
        help='end date in the format of YYYY-MM-DD')

    return parser.parse_args()

def crawl(start_date, end_date, output_folder):
    print('crawling data from {} to {}'.format(start_date, end_date))

    start_date, end_date = map(lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'), (start_date, end_date))

    date = start_date
    while date <= end_date:
        datestr = date.strftime('%Y-%m-%d')
        output_file = os.path.join(output_folder, datestr + '.csv')
        if os.path.exists(output_file):
            print('{} already exists, skip this date'.format(datestr))
        else:
            task = Task(datestr, output_file)
            task.start()
            task.join()
        date += datetime.timedelta(days=1)

if __name__ == "__main__":
    args = get_args()
    crawl(args.start_date, args.end_date, args.output_folder)