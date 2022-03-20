from os import getenv 
from json import loads
from smtplib import SMTP_SSL
from dotenv import load_dotenv
from email.mime.text import MIMEText
from ssl import create_default_context
from failed_alert import send_failed_alert
from mongo_collection import get_mongo_coll
from mismatch_alert import send_mismatch_alert
from email.mime.multipart import MIMEMultipart



load_dotenv()

ORG_LIST = loads(getenv('ORG_LIST'))
SMTP_PORT= getenv('SMTP_PORT')
SMTP_SERVER= getenv('SMTP_SERVER')
SENDER_EMAIL= getenv('SENDER_EMAIL')
RECIVER_EMAIL= getenv('RECIVER_EMAIL')
EMAIL_PASSWORD= getenv('EMAIL_PASSWORD')


def tracker(today_epoch,previous_day_epoch,day_of_week,yesterday_date,last_month,monthly_epoch):

    """
    tracker function sends Reports status mail
    
    today_epoch        : Epoch time for state date
    previous_day_epoch : Epoch time for end date
    day_of_week        : Day for the trigger day 
    yesterday_date     : Yesterday date in MM-DD-YYYY format
    last_month         : Month name 
    
    """

    tracker_message = MIMEMultipart("alternative")
    tracker_message["Subject"] = "Reports status[{yesterday_date}]".format(yesterday_date=yesterday_date)
    tracker_message["From"] = SENDER_EMAIL
    tracker_message["To"] = RECIVER_EMAIL

    report_coll, uns_coll, reportemail_coll, reportalert_coll = get_mongo_coll()

    def get_reports(ORG_LIST,triggerType,triggerDay):

        data = []
        for org,view in ORG_LIST:

            report_email_list = []
            active_users_emails = []
            estimated_reports = 0

            active_reports = report_coll.find({"reportStatus":"active","triggerType" : triggerType,"triggerDay" : triggerDay,"orgViewReq.organization" : org,"orgViewReq.view" : view,"createdOn" : {"$lt":previous_day_epoch}},{"loginInfo.providerKey":1})
            active_users = uns_coll.find({"activeStatus":True,"orgInfoList.name":org,"orgInfoList.viewInfoList.name":view},{"email":1})
            total_sent_reports = reportemail_coll.count({"emailSentTime":{"$gt":previous_day_epoch,"$lt":today_epoch},"emailStatus" : "c","triggerType":triggerType,"orgViewReq.organization":org,"orgViewReq.view":view})
            total_pending_reports = reportalert_coll.count({"executionOn":{"$gt":previous_day_epoch,"$lt":today_epoch},"triggerStatus" : "p","triggerType":triggerType,"orgViewReq.organization":org, "orgViewReq.view":view})
            total_inprogess_reports = reportalert_coll.count({"executionOn":{"$gt":previous_day_epoch,"$lt":today_epoch},"triggerStatus" : "i","triggerType":triggerType,"orgViewReq.organization":org,"orgViewReq.view":view})
            total_No_date_reports = reportalert_coll.count({"executionOn":{"$gt":previous_day_epoch,"$lt":today_epoch},"triggerStatus" : "ND","triggerType":triggerType,"orgViewReq.organization":org,"orgViewReq.view":view})
            total_failed_reports = reportalert_coll.count({"executionOn":{"$gt":previous_day_epoch,"$lt":today_epoch},"triggerStatus" : "f","triggerType":triggerType,"orgViewReq.organization":org,"orgViewReq.view":view})
                       
            for report in active_reports:
                report_email_list.append(report["loginInfo"]["providerKey"])
            
            for email in active_users:
                active_users_emails.append(email["email"])
            
            for count in report_email_list:
                if count in active_users_emails:
                    estimated_reports +=1
            
            result=[org, view, estimated_reports, total_sent_reports, total_pending_reports, total_inprogess_reports, total_No_date_reports, total_failed_reports]
            data.append(result)

        return data
    
    daily_reports = get_reports(ORG_LIST,"daily","everyday")
    weekly_reports = get_reports(ORG_LIST,"weekly",day_of_week)
    monthly_reports = []

    def table_row(rows):
        t=""
        for row in rows:
            if row[2] != sum(row[3:]):
                t+="""<tr style='background-color: #FFBBCC;'>
                    <td>"""+ row[0] +"""</td>
                    <td>"""+ row[1] +"""</td>
                    <td>"""+ str(row[2]) +"""</td>
                    <td>"""+ str(row[3]) +"""</td>
                    <td>"""+ str(row[4]) +"""</td>
                    <td>"""+ str(row[5]) +"""</td>
                    <td>"""+ str(row[6]) +"""</td>
                    <td>"""+ str(row[7]) +"""</td>
                </tr>"""
            else:
                t+="""<tr style='background-color: #E1E5EA'>
                <td>"""+ row[0] +"""</td>
                <td>"""+ row[1] +"""</td>
                <td>"""+ str(row[2]) +"""</td>
                <td>"""+ str(row[3]) +"""</td>
                <td>"""+ str(row[4]) +"""</td>
                <td>"""+ str(row[5]) +"""</td>
                <td>"""+ str(row[6]) +"""</td>
                <td>"""+ str(row[7]) +"""</td>
            </tr>"""
        return t

    monthly_table=""

    if today_epoch == monthly_epoch:
        monthly_reports = get_reports(ORG_LIST,"monthly","1st of Every Month")
        monthly_table = """
                <h2>Monthly report details:</h2>
                <h3>Month: {last_month} </h3>
                <table border='1' style='border-collapse:collapse;text-align: center; vertical-align: middle;'>
                    <tr style='background-color: #203239;color:#F7F7F7'>
                        <th>Organization</th>
                        <th>View</th>
                        <th>Monthly_Estimated_Mail</th>
                        <th>Total_Sent_email</th>
                        <th>Pending_Status</th>
                        <th>Inprocess_Status</th>
                        <th>No_Data</th>
                        <th>Failed_Status</th> 
                    </tr>""".format(last_month=last_month)
        monthly_table+=table_row(monthly_reports)
        monthly_table+="</table>"

    html = """\
                <html>
                    <body>
                        <h2>Daily report details:</h2>
                        <h3>Date: {yesterday_date} </h3>
                        <table border='1' style='border-collapse:collapse;text-align: center; vertical-align: middle;'>
                            <tr style='background-color: #203239;color:#F7F7F7'>
                                <th>Organization</th>
                                <th>View</th>
                                <th>Daily_Estimated_Mails</th>
                                <th>Total_Sent_Mails</th>
                                <th>Pending_Status</th>
                                <th>Inprocess_Status</th>
                                <th>No_Data</th>
                                <th>Failed_Status</th> 
                            </tr>
                            {Daily_Row}
                        </table>
                        <h2>Weekly report details for {day_of_week}:</h2>
                        <h3>Date: {yesterday_date} </h3>
                        <table border='1' style='border-collapse:collapse;text-align: center; vertical-align: middle;'>
                            <tr style='background-color: #203239;color:#F7F7F7'>
                                <th>Organization</th>
                                <th>View</th>
                                <th>Weekly_Estimated_Mail</th>
                                <th>Total_Sent_email</th>
                                <th>Pending_Status</th>
                                <th>Inprocess_Status</th>
                                <th>No_Data</th>
                                <th>Failed_Status</th> 
                            </tr>
                            {Weekly_Row}
                        </table>
                            {Monthly_Table}
                    </body>
                </html>
                """.format(Daily_Row = table_row(daily_reports),yesterday_date=yesterday_date,Weekly_Row=table_row(weekly_reports),day_of_week= day_of_week,Monthly_Table=monthly_table)

    part1 = MIMEText(html,"html")
    tracker_message.attach(part1)

    context = create_default_context()
    with SMTP_SSL(SMTP_SERVER,SMTP_PORT, context=context) as server:
        server.login(SENDER_EMAIL,EMAIL_PASSWORD)
        server.sendmail(SENDER_EMAIL,RECIVER_EMAIL,tracker_message.as_string())

    #sending mails for mismatch and failed reports
    all_data_list = [["daily","everyday",daily_reports ],["weekly",day_of_week, weekly_reports ],["montly","1st of Every Month",monthly_reports ]]
    send_mismatch_alert(all_data_list, previous_day_epoch, today_epoch, yesterday_date)
    send_failed_alert(all_data_list, previous_day_epoch, today_epoch, yesterday_date)
    

