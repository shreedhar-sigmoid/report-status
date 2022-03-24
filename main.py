from os import path
from report_status import tracker
from date_range import get_date_range
from datetime import date, datetime, timedelta
from pickle import load as pickle_load, dump as pickle_dump


today_epoch = str((int(date.today().strftime("%s")) * 1000) + 19800000)
data_lag_epoch = (int(date.today().strftime("%s")) * 1000) + 27000000
monthly_epoch = int(date.today().replace(day=2).strftime("%s")) * 1000 + 19800000

data_lag = get_date_range()

if data_lag != None:

    sort_data_lag = data_lag[:]
    sort_data_lag.sort()

    if sort_data_lag[0] >= data_lag_epoch:
        # This block excutes only data lag less than 2nd hour of today(UTC)

        if not path.exists("sendtime.pkl"):
            # This block will execute only when we run script first time or when
            # we remove the sendtime.pkl file and stores the email send time
            # in the sendtime.pkl file

            today_epoch = str((int(date.today().strftime("%s")) * 1000) + 19800000)
            previous_day_epoch = str((int((date.today() - timedelta(days=1)).strftime("%s")) * 1000) + 19800000)
            day_of_week = datetime.fromtimestamp(int(previous_day_epoch) / 1000).strftime("%A").lower()
            yesterday_date = (date.today() - timedelta(days=1)).strftime("%d-%m-%Y")
            last_month = (date.today().replace(day=1) - timedelta(days=1)).strftime("%B")

            tracker(
                today_epoch,
                previous_day_epoch,
                day_of_week,
                yesterday_date,
                last_month,
                monthly_epoch,
            )

            email_sent_time = today_epoch
            with open("sendtime.pkl", "wb") as file:
                pickle_dump(email_sent_time, file)  # Storing the email sent time in sendtime.pkl

        else:
            email_sent_time = pickle_load(open("sendtime.pkl", "rb"))
            if int(email_sent_time) < int(today_epoch):

                days = int((int(today_epoch) - int(email_sent_time)) / 86400000)

                for day in reversed(range(1,days)):
                    # Due to data lag if email din't triggered for more than one day then,
                    # this block will sends report status from last time report status triggered

                    today_epoch = str((int((date.today() - timedelta(days=day - 1)).strftime("%s")) * 1000)+ 19800000)
                    previous_day_epoch = str((int((date.today() - timedelta(days=day)).strftime("%s"))* 1000)+ 19800000)
                    day_of_week = datetime.fromtimestamp(int(previous_day_epoch) / 1000).strftime("%A").lower()
                    yesterday_date = (date.today() - timedelta(days=day)).strftime("%d-%m-%Y")
                    last_month = ((date.today() - timedelta(days=day + 1)).replace(day=1) - timedelta(days=1)).strftime("%B")

                    tracker(
                        today_epoch,
                        previous_day_epoch,
                        day_of_week,
                        yesterday_date,
                        last_month,
                        monthly_epoch,
                    )

                email_sent_time = str((int(date.today().strftime("%s")) * 1000) + 19800000)
                with open("sendtime.pkl", "wb") as file:
                    pickle_dump(email_sent_time, file)
