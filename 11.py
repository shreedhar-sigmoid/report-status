from pickle import load as pickle_load, dump as pickle_dump
from datetime import date ,timedelta, datetime

# today_epoch = (int(date.today().strftime("%s")) *1000)+19800000

email_sent_time = str(1647475200000)
with open("sendtime.pkl", "wb") as file:
    pickle_dump(email_sent_time, file)



# today_epoch = str((int(date.today().strftime("%s")) *1000)+19800000)
# previous_day_epoch = str((int((date.today() - timedelta(days=1)).strftime("%s")) *1000)+19800000)   
# day_of_week = datetime.fromtimestamp(int(previous_day_epoch)/1000).strftime("%A").lower()
# yesterday_date = (date.today() - timedelta(days = 1)).strftime("%d-%m-%Y")
# last_month = (date.today().replace(day = 1) - timedelta(days=1)).strftime("%B")  
# print(today_epoch,previous_day_epoch,day_of_week,yesterday_date,last_month)
# for day in range(1,1+2):
#                     today_epoch = str((int((date.today() - timedelta(days=day-1)).strftime("%s")) *1000)+19800000) 
#                     previous_day_epoch = str((int((date.today() - timedelta(days=day)).strftime("%s")) *1000)+19800000) 
#                     day_of_week = datetime.fromtimestamp(int(previous_day_epoch)/1000).strftime("%A").lower()
#                     yesterday_date = (date.today() - timedelta(days = day)).strftime("%d-%m-%Y")
#                     last_month = ((date.today() - timedelta(days=day+1)).replace(day = 1) - timedelta(days=1)).strftime("%B")
#                     print(today_epoch,previous_day_epoch,day_of_week,yesterday_date,last_month)
