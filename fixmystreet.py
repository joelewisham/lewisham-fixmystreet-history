import requests
from datetime import date, datetime, timedelta
import sqlite_utils
from flatten_json import flatten

# SETUP DATABASE, TABLE AND SCHEMA
db = sqlite_utils.Database("fixmystreet.db")
tablename = 'requests'
table = db.table(
    tablename,
    pk=('service_request_id'),
    not_null={'service_request_id'},
    column_order=('service_name','agency_responsible_recipient_0','service_request_id','service_code','long','lat','comment_count','title','requested_datetime','agency_sent_datetime','description','status','interface_used','detail','updated_datetime','media_url','requestor_name')
)

# PREPARE TO SCAN DATA FOR THE LAST 1 WEEK
EndDate = date.today() + timedelta(days = 1)
EndWeekDate = EndDate
StartWeekDate = EndDate - timedelta(weeks = 1)
StartDate = StartWeekDate - timedelta(days = 1)

# GET THE JSON DATA, FLATTEN IT AND UPSERT INTO THE FULL HISTORY DATABASE
requestsFlat = []
# LEWISHAM ID FROM MAPIT.MYSOCIETY.ORG
mapitID=2492
while StartWeekDate > StartDate:
    url = f'https://www.fixmystreet.com/open311/v2/requests.json?jurisdiction_id=fixmystreet&agency_responsible={mapitID}&start_date={StartDate.strftime("%Y-%m-%d")}&end_date={EndDate.strftime("%Y-%m-%d")}'
    req = requests.get(url, headers={'Connection':'close'})
    j = req.json()
    for req in j['service_requests']:
        requestsFlat.append(flatten(req))
    EndWeekDate = StartWeekDate
    StartWeekDate = EndWeekDate - timedelta(weeks = 1)

db[tablename].upsert_all(requestsFlat,pk=('service_request_id'))
