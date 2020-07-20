import json
from datetime import datetime
import argparse

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials


def include_in(row):
    if row['QwatchID'].startswith('P'):
        return 'Yes'
    elif (datetime.today() - datetime.strptime(row['DateQurantine'], '%d-%m-%Y')).days in [2,5,10]:
        return 'Yes'
    else:
        return 'No'


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('daily_data', type=str, help='path to the daily input data')
parser.add_argument('credentials', type=str, help='google drive credential file')
parser.add_argument('google_sheet_file', type=str, help='google sheet id file')
args = parser.parse_args()
credentials = args.credentials
daily_data = args.daily_data
google_sheet_file = args.google_sheet_file
# read the file
df = pd.read_csv(daily_data)
# read only HQ entries
df = df[df['QuarantineType'] == 'HQ']
# Define Quarantine status as Ongoing or Complete depending upon today's date to filter out completed quarantines
df['Quarantine Status'] = df['EndDateQurantine'].apply(lambda x: 'Ongoing' if (datetime.today() - datetime.strptime(x, '%d-%m-%Y')).days <= 0 else 'Complete')
# Filter only ongoing quarantines
df = df[df['Quarantine Status']=='Ongoing']
# filter if QwatchID starts with P else if it's 2, 5, 10 days after quarantine started
df['include'] = df.apply(include_in, axis=1)
df = df[df['include']=='Yes']

# Convert mobile type from float to str without decimal
df['MOBILE'] = df['Mobile'].apply(lambda x: str(x).split('.')[0])
# Compute Number of days completed in quarantine
df['Qdays'] = df['DateQurantine'].apply(lambda x: (datetime.today() - datetime.strptime(x, '%d-%m-%Y')).days)
# Fill the empty rows for the following columns with blank space to be able to merge columns
df.fillna('', inplace=True)
# Merge the Address1, Address2, Address3 columns into one and remove empty ", "s
df['ADDRESS'] = df['Address1'] + ', ' + df['Address2'] + ', ' + df['Address3']
df['ADDRESS'] = df['ADDRESS'].apply(lambda x: x.strip(', ').strip(', '))
# Merge the BBMPZoneName and TalukaZone columns into one and remove empty " / "s
df['Zone / TALUK'] = df['BBMPZoneName'] + ' / ' + df['TalukaName']
df['Zone / TALUK'] = df['Zone / TALUK'].apply(lambda x: x.strip(' / '))
# Merge the WardName and PanchayatName columns into one and remove empty " / "s
df['Ward / Panchayat / hobli'] = df['WardName'] + ' / ' + df['PanchayatName']
df['Ward / Panchayat / hobli'] = df['Ward / Panchayat / hobli'].apply(lambda x: x.strip(' / '))
# These columns are not in daily data. For now, create empty cols, if from another source then perform join on QwatchID
# df['P Type'] = ''
# df['Details'] = ''
# df['Changed address'] = ''
# df['Changed Number'] = ''

# Select only required columns
df = df[['QwatchID', 'Name', 'GenderName', 'MOBILE', 'DateQurantine', 'Qdays', 'ADDRESS', 'Age', 
         'Zone / TALUK', 'Ward / Panchayat / hobli', 'Citz_FromState', 'AddressTypeName']]

# Rename necessary columns
df.rename({
    'QwatchID': 'Qwatch',
    'Name': 'NAME'
}, axis=1, inplace=True)

# comment this next line to run for all zones; Note, you'll need to uncomment lines 88-90 for it
df = df[df['Zone / TALUK']=='BOMMANAHALLI']

with open(google_sheet_file, 'r') as f:
    google_sheet_ids = json.loads(f.read())

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(credentials, SCOPES)
client = gspread.authorize(creds)

for zone in df['Zone / TALUK'].unique():
    # for each zone, get required rows    
    zone_df = df[df['Zone / TALUK']==zone]
    # get google sheet id for zone
    try:
        sheet = client.open_by_key(google_sheet_ids[zone])
    # if no sheet for zone, then create one and share with HQ team
    except KeyError as ke:
#         sheet = client.create(zone)
#         google_sheet_ids[zone] = sheet.id
#         sheet.share('covid19karnataka@gmail.com', perm_type='user', role='owner')
        continue
    for ward in sorted(zone_df['Ward / Panchayat / hobli'].unique()):
        # for each ward in zone, get required rows
        sub_df = zone_df[zone_df['Ward / Panchayat / hobli']==ward]
        if ward == '':
            ward = 'Null'
        # read the sheet for ward
        try:
            worksheet = sheet.worksheet(ward)
        # if no sheet for ward, then create one with headers
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=ward, rows="100", cols="30")
        # clear existing data
        worksheet.clear()
        # add header row
        worksheet.append_rows([sub_df.columns.tolist()])
        # add rows
        worksheet.append_rows(sub_df.values.tolist())
# write the updated sheet ID list back to file
with open(google_sheet_file, 'w+') as f:
    json.dump(google_sheet_ids, f)
