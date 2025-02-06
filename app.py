import datetime as dt
from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import streamlit as st
from math import pi
from bokeh.palettes import Category20c
from bokeh.plotting import figure
from bokeh.palettes import Category20c
from bokeh.transform import cumsum
import matplotlib.pyplot as plt 
import seaborn 

load_dotenv()

# DATABASE CONSTANTS
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = 5432
DB_NAME = 'pagila'
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

# Connect to Database 

connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)
conn = engine.connect()

print('Connected to Pagila Database')

# st.write("streamlit version = {}".format(st.__version__))

# Custom Classes
class Dance:
    # name = ''
    # isGroupDance = False
    # num_of_scores = 0
    scores = []

    def __init__(self, name: str, isGroupDance: bool, scores: dict):
        self.name = name
        self.isGroupDance = isGroupDance
        self.num_of_scores = len(scores)
        self.scores = scores

    # score could be mark (unlikely for final), final (which is rank based) or onScale3

    # def avg(scores):
    #     if scores


class Score:
    breakdown = {}

    def __init__(self, kind: str, adjudicator: int, breakdown: dict = None):
        self.kind = kind
        self.adjudicator = adjudicator
        if breakdown != None:
            self.breakdown = breakdown

        def avg(breakdown):
            if kind.strip() == "onScale3":
                total = 0
                for key in breakdown.keys():
                    key.strip()
                    if key in ("tq", "mm", "ps", "cp"):
                        total += float(breakdown["key"].strip())
            return
            
            # for key in breakdown.keys():


# Functions

def extract_result_details(details : str):   

    details_dict = {"score":{}}

    for index, detail in enumerate(details.split('|')):
        if index == 0 :
            if len(detail.split[':']) == 2:
                details_dict["name"] = detail.split(':')[1]
        elif index == 1:
            if len(detail.split[':']) == 2:
                details_dict["name"] = detail.split(':')[1]
        else:
            detail = detail.replace('score :', '')
            for str in detail.split(';'):
                key_value_pair = str.split(':')
                print(key_value_pair)
                if len(key_value_pair) == 2:
                    details_dict["score"][f"{key_value_pair[0]}"] = key_value_pair[1]
    
    return details_dict

# all_details = pd.read_sql("SELECT details FROM student.results;", conn)
# for detail in all_details["details"]:
#     x = extract_result_details(detail)
#     print(x)

# Plotting

# M & F Age vs Final Rank
# Competitors Age vs Final Rank

female_age_vs_final_ranking_query = '''
SELECT
	r.couple_id,
	r.rank,
	cm.id,
	cm.year_of_birth
FROM 
	student.results r
LEFT JOIN
	student.couples cp ON cp.id = r.couple_id
LEFT JOIN
	student.competitors cm ON cm.id = cp.male_id;
'''

num_of_champions_and_country_they_represent_query = '''
SELECT
	cp.country,
	COUNT(cp.country) AS num_of_wins,
	COUNT(DISTINCT r.couple_id) AS num_of_couples
FROM 
	student.results r
LEFT JOIN
	student.couples cp ON cp.id = r.couple_id
WHERE
	r.rank = 1
GROUP BY
	cp.country
ORDER BY
	COUNT(cp.country) DESC, cp.country;
'''

st.markdown('''# DANCE ANALYTICA''')

df = pd.read_sql(female_age_vs_final_ranking_query, conn)
df1 = pd.read_sql(num_of_champions_and_country_they_represent_query, conn)

age = df["year_of_birth"]
rank = df["rank"]

plot = figure(title = 'Age v. Final Ranking (Female)',
              x_axis_label = 'Rank',
              y_axis_label = 'Age')
# plot.x_range.flipped
plot.scatter(rank, age, size=10)
st.bokeh_chart(plot)

# countries = df1["country"]
# x = {countries[index]: wins for index, wins in enumerate(df1["num_of_wins"])}

# Pie Chart
labels = df1["country"]
sizes = df1["num_of_wins"]

fig2, ax = plt.subplots()
ax.pie(sizes, labels=labels, labeldistance= 1.3, startangle=-90)

st.subheader('Number of Champions by Country')
st.caption("Find out which  country boots the most champions since the start of 2024!")
st.pyplot(fig2)

st.markdown('''---''')

# st.map(df1["country"])

# Close connection to Database

conn.close()

print('Database connection closed')

