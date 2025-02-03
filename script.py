import os
import requests
import pandas as pd
import datetime as dt
from dotenv import load_dotenv

load_dotenv()

# Connect to API

present_date = str(dt.datetime.now().date()).replace('-', '/')

def make_request(url: str, auth: tuple, path : str = None, query_parameters : str = None):
    '''
        Function returns a request object from up to 3 parameters.
    '''

    try:
        return requests.get(f"{url}{path}?{query_parameters}", auth=auth)
    except ImportError as e: 
        return f"{e.name}: please import requests library before calling this function again."

# Requests (not all)
URL = os.environ.get("API_ADDRESS")
USERNAME = os.environ.get("API_USERNAME")
PASSWORD = os.environ.get("API_PASSWORD")

competitions_request = make_request(
    URL,
    (USERNAME, PASSWORD),
    "competition",
    f"from=2024/01/01&to={present_date}&division=General&status=Closed&format=json"
)

# print(request.status_code)

competitions_df = pd.DataFrame(competitions_request.json())

competitions_df = competitions_df[competitions_df['name'].str.contains('ADULT')]
competitions_df = competitions_df[competitions_df['name'].str.contains('TEAM|SOLO|SYN|CHOR|FORMATION') == False]

competitions_data = []

for index, row in competitions_df.iterrows():
    competition_id = row['id']
    details= row['name'].split('-')

    name = details[0].strip()
    city = details[1].strip()
    country = details[2].strip()
    date = details[3].strip()

    discipline = ''
    if 'STANDARD  ADULT' in name:
        discipline = 'STANDARD'
    elif 'LATIN  ADULT' in name:
        discipline = 'LATIN'
    else:
        continue

    competitions_data.append([competition_id, name, city, country, date, discipline])

competitions_table=pd.DataFrame(competitions_data)
competitions_table.columns = ['Competition ID', 'Name', 'City', 'Country', 'Date', 'Discipline']
# print(competitions_table)

results_data = []
couple_id_list = []

for index, competition in competitions_df.iterrows():
    result_request = make_request(
        URL,
        (USERNAME, PASSWORD),
        "participant",
        f"competitionID={competition['id']}&format=json",
    )
    # print(competition['id'])
    result_df = pd.DataFrame(result_request.json())
    # print(competition['id'])
    result_df = result_df[result_df['status'].str.contains('Present')].head(6)

    # print(result_df)
    # print(result_df.head(6))

    rank = 0

    for index, result in result_df.iterrows():

        participant_request = make_request(
            URL,
            (USERNAME, PASSWORD),
            f"participant/{result['id']}",
            "format=json"
        )

        participant_info = participant_request.json()

        # print(result['id'])
        couple_id = participant_info['coupleId']
        rank += 1
        competition_id = competition['id']
        details = participant_info['rounds'][-2::]

        for round in participant_info['rounds']:
            if round['name'] == 'F':
                details = round['dances']

        results_data.append([couple_id, rank, competition_id, details])
        
        if couple_id not in couple_id_list:
            couple_id_list.append(couple_id)

        # print(couple_id_list)

couples_data = []
competitors_id_list = []

for couple_id in couple_id_list:
    couple_request = make_request(
        URL,
        (USERNAME, PASSWORD),
        f"couple/{couple_id}",
        "format=json"
    )

    print(couple_id)
    couple_name = couple_request.json()["name"]
    country = couple_request.json()["country"]
    male_id = couple_request.json()["man"]
    female_id = couple_request.json()["woman"]

    couples_data.append([couple_id, couple_name, country, male_id, female_id])
    competitors_id_list.append(male_id)
    competitors_id_list.append(female_id)

competitors_data = []

for competitor_id in competitors_id_list:
    competitor_request = make_request(
        URL,
        (USERNAME, PASSWORD),
        f"person/{competitor_id}",
        "format=json"
    )

    print(competitor_id)
    name = competitor_request.json()["name"]
    surname = competitor_request.json()["surname"]
    sex = competitor_request.json()["sex"]
    nationality = competitor_request.json()["nationality"]
    country = competitor_request.json()["country"]
    year_of_birth = competitor_request.json()["yearOfBirth"]

    competitors_data.append([competitor_id, name, surname, sex, nationality, country, year_of_birth])

print(pd.DataFrame(competitions_data))
print('complete')
print(pd.DataFrame(results_data))
print('complete')
print(pd.DataFrame(couples_data))
print('complete')
print(pd.DataFrame(competitors_data))
print('complete')

# for 


# Clean Data


# Load into Database