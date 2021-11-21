from pandas.io.parsers import read_csv
import schedule
import datetime
import pandas as pd
import time
import requests
import os

class TopGainersLosers:
    def __init__(self, count, file_name) -> None:
        self.count = count
        self.file_name = file_name
        self.root_url = "https://www.nseindia.com/"
        self.urls = {
            "NIFTY-F&O-gainers": f"{self.root_url}api/live-analysis-variations?index=gainers",
            "NIFTY-F&O-loosers": f"{self.root_url}api/live-analysis-variations?index=loosers",
            "VOLUME" : f"{self.root_url}api/live-analysis-volume-gainers?mode=laVolumeGainer"
        }

        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                                'like Gecko) '
                                'Chrome/80.0.3987.149 Safari/537.36',
                'accept-language': 'en,gu;q=0.9,hi;q=0.8', 'accept-encoding': 'gzip, deflate, br'}
    

    def _download_csv(self, url):
        session = requests.Session()
        request = session.get(self.root_url, headers=self.headers, timeout=5)
        cookies = dict(request.cookies)
        response = session.get(url, headers=self.headers, timeout=5, cookies=cookies)
        return response.json()

    def load_save_csv(self, out_dir):
       
        for category in self.urls:
            _response_json = self._download_csv(self.urls[category])
            if category == "VOLUME":
                file_name = f"{out_dir}/VOLUME-GAINERS.csv"
                print(file_name)
                df = pd.DataFrame.from_records(_response_json["data"])
                df.to_csv(file_name, sep='\t', encoding='utf-8')
            else:
                for type in ["NIFTY", "BANKNIFTY", "FOSec"]:
                    if "gainers" in category:
                        file_name = f"{out_dir}/{type}-GAINERS.csv"
                    else:
                        file_name = f"{out_dir}/{type}-LOOSERS.csv"
                    df = pd.DataFrame.from_records(_response_json[type]["data"])
                    df.to_csv(file_name, sep='\t', encoding='utf-8')
                    print(file_name)

    

    def main(self):
        current_time = datetime.datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
        outdir = f"reports/{datetime.date.today().strftime('%d-%m-%Y')}/{current_time}"
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        self.load_save_csv(outdir)



class ScheduleJob:
    @staticmethod
    def calculate_schedule_timings(start_time, end_time, frequency):
        data_start = datetime.date.today().strftime('%d/%m/%Y %H:%M:%S')
        day_end = datetime.datetime.combine(datetime.datetime.today(),
                                            datetime.time(23, 59, 59, 999999)).strftime('%d/%m/%Y %H:%M:%S')
        schedule_times = (pd.DataFrame(columns=['NULL'],
                                       index=pd.date_range(data_start, day_end, freq=f'{frequency}T'))
                          .between_time(start_time, end_time)
                          .index.strftime('%H:%M')
                          .tolist()
                          )
        return schedule_times

    @staticmethod
    def schedule_job(time, job):
        schedule.every().day.at(time).do(job)

    @staticmethod
    def scheduler_run_pending():
        schedule.run_pending()

if __name__ == "__main__":
    toppers_count = 10
    file_name = "top_gainers_losers.txt"
    fetcher = TopGainersLosers(toppers_count, file_name)
    fetcher.main()

    start_time = "09:15"
    end_time = "15:30"
    frequency = "15"
    scheduled_times = ScheduleJob.calculate_schedule_timings(start_time, end_time, frequency)
    print(scheduled_times)

    for schedule_time in scheduled_times:
        ScheduleJob.schedule_job(schedule_time, fetcher.main)
        

    while True:
        ScheduleJob.scheduler_run_pending()
        time.sleep(1)
