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
    def __init__(self, date, output_folder):
        super().__init__()
        self.__results = []
        self.__datestr = date.strftime('%Y-%m-%d')
        self.__output_folder = output_folder

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
            timer = threading.Timer(2, self.__close)
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
        df.to_csv(os.path.join(self.__output_folder, self.__datestr + '.csv'))
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

    tasks = []
    date = start_date

    while date <= end_date:
        tasks.append(Task(date, output_folder))
        date += datetime.timedelta(days=1)

    for task in tasks:
        task.start()

    for task in tasks:
        task.join()

if __name__ == "__main__":
    args = get_args()

    crawl(args.start_date, args.end_date, args.output_folder)