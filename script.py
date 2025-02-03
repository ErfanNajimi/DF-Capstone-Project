import datetime as dt
from dotenv import load_dotenv
import os
import pandas as pd
import requests
import psycopg2
from sqlalchemy import create_engine

load_dotenv()

# DATABASE CONSTANTS
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = 5432
DB_NAME = 'pagila'
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

# API CONSTANTS
URL = os.environ.get("API_ADDRESS")
USERNAME = os.environ.get("API_USERNAME")
PASSWORD = os.environ.get("API_PASSWORD")

# Connect to Database 

connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)
conn = engine.connect()

print('CONNECTED TO PAGILA DATABASE.')

conn.execute("CREATE TABLE IF NOT EXISTS competitions (id INT PRIMARY KEY, name TEXT, city TEXT, country TEXT, date DATE, discipline VARCHAR(8));")

conn.execute("CREATE TABLE IF NOT EXISTS competitors (id INT PRIMARY KEY, first_name TEXT, second_name TEXT, sex TEXT, nationality TEXT, country TEXT, year_of_birth INT);")

conn.execute("CREATE TABLE IF NOT EXISTS couples (id INT PRIMARY KEY, name TEXT, country TEXT, male_id INT NOT NULL, female_id INT NOT NULL, FOREIGN KEY (male_id) REFERENCES competitors(id), FOREIGN KEY (female_id) REFERENCES competitors(id));")

conn.execute("CREATE TABLE IF NOT EXISTS results (id INT PRIMARY KEY, couple_id INT NOT NULL, rank INT, competition_id INT NOT NULL, details TEXT, FOREIGN KEY (couple_id) REFERENCES couples(id), FOREIGN KEY (competition_id) REFERENCES competitions(id));")

conn.commit()

start_date = "" 

latest_comp_date = pd.read_sql("SELECT date FROM competitions ORDER BY date DESC LIMIT 1", conn)
if latest_comp_date.empty:
    start_date = "2024/01/01" 
else: 
    start_date = latest_comp_date["date"][0]
    datetime_object = dt.datetime.strptime(start_date, '%Y/%m/%d') + dt.timedelta(days=1)
    start_date = datetime_object.strftime('%Y/%m/%d')

print(start_date)

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

competitions_request = make_request(
    URL,
    (USERNAME, PASSWORD),
    "competition",
    f"from={start_date}&to={present_date}&division=General&status=Closed&format=json"
)

# print(request.status_code)

competitions_df = pd.DataFrame(competitions_request.json())

if competitions_df.empty != True:
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

    print('COMPLETE: Competitions data collected.')

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

    print('COMPLETE: Results data collected.')

    couples_data = []
    competitors_id_list = []

    for couple_id in couple_id_list:
        couple_request = make_request(
            URL,
            (USERNAME, PASSWORD),
            f"couple/{couple_id}",
            "format=json"
        )

        couple_name = couple_request.json()["name"]
        country = couple_request.json()["country"]
        male_id = couple_request.json()["man"]
        female_id = couple_request.json()["woman"]

        couples_data.append([couple_id, couple_name, country, male_id, female_id])
        competitors_id_list.append(male_id)
        competitors_id_list.append(female_id)

    print('COMPLETE: Couples data collected.')

    competitors_data = []

    for competitor_id in competitors_id_list:
        competitor_request = make_request(
            URL,
            (USERNAME, PASSWORD),
            f"person/{competitor_id}",
            "format=json"
        )

        name = competitor_request.json()["name"]
        surname = competitor_request.json()["surname"]
        sex = competitor_request.json()["sex"]
        nationality = competitor_request.json()["nationality"]
        country = competitor_request.json()["country"]
        year_of_birth = competitor_request.json()["yearOfBirth"]

        competitors_data.append([competitor_id, name, surname, sex, nationality, country, year_of_birth])

    print('COMPLETE: Competitors data collected.')

    for competition in competitions_data:
        conn.execute(f"INSERT INTO competitions (id, name, city, country, date, discipline) VALUES {tuple(competition)}")

    for competitor in competitors_data:
        conn.execute(f"INSERT INTO competitors (id, first_name, second_name, sex, nationality, country, year_of_birth) VALUES {tuple(competitor)}")

    for couple in couples_data:
        conn.execute(f"INSERT INTO couples (id, name, country, male_id, female_id) VALUES {tuple(couple)}")

    for result in results_data:
        conn.execute(f"INSERT INTO results (id, couple_id, rank, competition_id, details) VALUES {tuple(result)}")

else:
    print('No new data to collect.')

conn.commit()

conn.close()