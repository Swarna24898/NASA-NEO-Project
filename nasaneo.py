


import pandas as pd

from datetime import datetime

from datetime import date

import streamlit as st

import pymysql

st.set_page_config(page_title="NASA ASTEROIDS NEAR EARTH OBJECTS")

st.title("NASA ASTEROIDS:rocket: - Project 1")

def get_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="1234",
        database="nasa"
    )

query_options = [
    "Show all columns from both tables",
    "1. Count how many times each asteroid has approached Earth",
    "2. Average velocity of each asteroid over multiple approaches",
    "3. List top 10 fastest asteroids",
    "4. Find potentially hazardous asteroids that have approached Earth more than 3 times",
    "5. Find the month with the most asteroid approaches",
    "6. Get the asteroid with the fastest ever approach speed",
    "7. Sort asteroids by maximum estimated diameter (descending)",
    "8. Asteroids whose closest approach is getting nearer over time",
    "9. Display the name of each asteroid along with the date and miss distance of its closest approach to Earth.",
    "10. List names of asteroids that approached Earth with velocity > 50,000 km/h",
    "11. Count how many approaches happened per month",
    "12. Find asteroid with the highest brightness (lowest magnitude value)",
    "13. Get number of hazardous vs non-hazardous asteroids",
    "14. Find asteroids that passed closer than the Moon (lesser than 1 LD), along with their close approach date and distance.",
    "15. Find asteroids that came within 0.05 AU(astronomical distance)",
    "16. List all asteroid names",
    "17. Get total number of potentially hazardous asteroids",
    "18. Find asteroid with maximum estimated diameter",
    "19. Get total number of unique asteroids tracked",
    "20. Find asteroids that approached Earth more than once on the same day",
    "21. Compare average miss distance of hazardous vs non-hazardous asteroids",
    "22. Get total number of close approaches recorded",
    "23. Average Miss Distance of Hazardous Asteroids",
    "24. Top 5 Asteroids That Came Closest to Earth",
    "25. List all unique orbiting bodies"
]

st.sidebar.header("Select Criteria")

selected_query = st.sidebar.selectbox("Choose a query", query_options)

# Query mapping

query_map = {

    "Show all columns from both tables": """
    SELECT * from asteroids A
    INNER JOIN close_approach B
    ON A.id = B.neo_ref_id
    LIMIT 1000;""",

    "1. Count how many times each asteroid has approached Earth": """ 
    SELECT A.name, COUNT(*) AS approach_count
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    GROUP BY A.name 
    ORDER BY approach_count DESC;""",

    "2. Average velocity of each asteroid over multiple approaches": """
    SELECT A.name, AVG(B.relative_velocity_kmph) AS avg_velocity
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    GROUP BY A.name
    ORDER BY avg_velocity DESC;""",

    "3. List top 10 fastest asteroids": """
    SELECT A.name, MAX(B.relative_velocity_kmph) AS max_velocity
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    GROUP BY A.name
    ORDER BY max_velocity DESC
    LIMIT 10;""",

    "4. Find potentially hazardous asteroids that have approached Earth more than 3 times": """
    SELECT A.name, COUNT(*) AS approach_count
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    WHERE A.is_potentially_hazardous_asteroid = TRUE
    GROUP BY A.name
    HAVING approach_count > 3
    ORDER BY approach_count DESC;""",

    "5. Find the month with the most asteroid approaches": """ 
    SELECT MONTH(B.close_approach_date) AS month, COUNT(*) AS approach_count
    FROM close_approach B
    GROUP BY MONTH(B.close_approach_date)
    ORDER BY approach_count DESC
    LIMIT 10;""",

    "6. Get the asteroid with the fastest ever approach speed": """
    SELECT A.name, B.relative_velocity_kmph
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    ORDER BY B.relative_velocity_kmph DESC
    LIMIT 1;""",

    "7. Sort asteroids by maximum estimated diameter (descending)": """
    SELECT name, estimated_diameter_max_km
    FROM asteroids
    ORDER BY estimated_diameter_max_km DESC;""",

    "8. Asteroids whose closest approach is getting nearer over time": """
    SELECT A.name, B.close_approach_date, B.miss_distance_km
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    ORDER BY A.name, B.close_approach_date DESC
    LIMIT 50; """,
    
    "9. Display the name of each asteroid along with the date and miss distance of its closest approach to Earth.": """
    SELECT A.name, B.close_approach_date, B.miss_distance_km
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    ORDER BY miss_distance_km ASC
    LIMIT 50;""",

    "10. List names of asteroids that approached Earth with velocity > 50,000 km/h": """
    SELECT A.name, B.relative_velocity_kmph
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    WHERE B.relative_velocity_kmph > 50000;""",

    "11. Count how many approaches happened per month": """
    SELECT MONTH(B.close_approach_date) AS month, COUNT(*) AS approach_count
    FROM close_approach B
    GROUP BY MONTH(B.close_approach_date)
    ORDER BY month;""",
   
    "12. Find asteroid with the highest brightness (lowest magnitude value)": """
    SELECT name, absolute_magnitude_h
    FROM asteroids A
    ORDER BY absolute_magnitude_h ASC
    LIMIT 10; """,

    "13. Get number of hazardous vs non-hazardous asteroids": """
    SELECT is_potentially_hazardous_asteroid, COUNT(*) AS count
    FROM asteroids
    GROUP BY is_potentially_hazardous_asteroid; """,

    "14. Find asteroids that passed closer than the Moon (lesser than 1 LD), along with their close approach date and distance.": """
    SELECT A.name, B.close_approach_date, B.miss_distance_lunar
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    WHERE B.miss_distance_lunar < 1; """,

    "15. Find asteroids that came within 0.05 AU(astronomical distance)": """
    SELECT A.name, B.close_approach_date, B.astronomical
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    WHERE B.astronomical < 0.05; """,

    "16. List all asteroid names": """
    SELECT DISTINCT name FROM asteroids;""",

    "17. Get total number of potentially hazardous asteroids": """
    SELECT COUNT(*) AS hazardous_count
    FROM asteroids
    WHERE is_potentially_hazardous_asteroid = 1;""",

    "18. Find asteroid with maximum estimated diameter": """
    SELECT name, estimated_diameter_max_km
    FROM asteroids
    ORDER BY estimated_diameter_max_km DESC
    LIMIT 1;""",

    "19. Get total number of unique asteroids tracked": """
    SELECT COUNT(DISTINCT A.id) AS total_asteroids
    FROM asteroids A;""",

    "20. Find asteroids that approached Earth more than once on the same day": """
    SELECT B.close_approach_date, A.name, COUNT(*) AS times_same_day
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    GROUP BY B.close_approach_date, A.name
    HAVING times_same_day > 1
    ORDER BY B.close_approach_date;""",

    "21. Compare average miss distance of hazardous vs non-hazardous asteroids": """
    SELECT 
      A.is_potentially_hazardous_asteroid,
      AVG(B.miss_distance_km) AS avg_miss_distance
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    GROUP BY A.is_potentially_hazardous_asteroid;""",

    "22. Get total number of close approaches recorded": """
    SELECT COUNT(*) AS total_approaches FROM close_approach;""",

    "23. Average Miss Distance of Hazardous Asteroids": """
    SELECT AVG(B.miss_distance_km) AS avg_miss_distance
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    WHERE A.is_potentially_hazardous_asteroid = 1;""",

    "24. Top 5 Asteroids That Came Closest to Earth": """
    SELECT A.name, B.close_approach_date, B.miss_distance_km
    FROM asteroids A
    JOIN close_approach B ON A.id = B.neo_ref_id
    ORDER BY B.miss_distance_km ASC
    LIMIT 5;""",

    "25. List all unique orbiting bodies": """
    SELECT DISTINCT orbiting_body FROM close_approach;"""
    }

# Filters

st.sidebar.header("Filter asteroid data")

close_approach_start_date = st.sidebar.slider("Close Approach Start Date", min_value=date(2020, 1, 1), max_value=date(2020, 1, 30), value=date(2020, 1, 1))
close_approach_end_date = st.sidebar.slider("Close Approach End Date", min_value=date(2020, 1, 8), max_value=date(2020, 1, 31), value=date(2020, 1, 15))

astronomical_units = st.sidebar.slider("Astronomical Units", min_value=0.0, max_value=1.0, value=(0.0, 0.5))
lunar_distance = st.sidebar.slider("Lunar Distance", min_value=0.0, max_value=400.0, value=(0.0, 100.0))
relative_velocity = st.sidebar.slider("Relative Velocity (kmph)", min_value=0.0, max_value=150000.0, value=(0.0, 80000.0))
estimated_diameter_min = st.sidebar.slider("Estimated Diameter Min (km)", min_value=0.0, max_value=10.0, value=(0.0, 10.0))
estimated_diameter_max = st.sidebar.slider("Estimated Diameter Max (km)", min_value=0.0, max_value=20.0, value=(0.0, 20.0))
hazardous_state = st.sidebar.slider("Hazardous Asteroid (0 = No, 1 = Yes)", min_value=0, max_value=1, value=0, step=1)

# Only apply filters if the selected query is the base data fetch
if selected_query == "Show all columns from both tables":
    base_query = """
        SELECT A.name, A.estimated_diameter_min_km, A.estimated_diameter_max_km,
               A.is_potentially_hazardous_asteroid,
               B.close_approach_date, B.relative_velocity_kmph,
               B.miss_distance_km, B.astronomical, B.miss_distance_lunar
        FROM asteroids A
        JOIN close_approach B ON A.id = B.neo_ref_id
    """

filters = f"""
WHERE B.close_approach_date BETWEEN '{close_approach_start_date}' AND '{close_approach_end_date}'
  AND B.astronomical BETWEEN {astronomical_units[0]} AND {astronomical_units[1]}
  AND B.miss_distance_lunar BETWEEN {lunar_distance[0]} AND {lunar_distance[1]}
  AND B.relative_velocity_kmph BETWEEN {relative_velocity[0]} AND {relative_velocity[1]}
  AND A.estimated_diameter_min_km BETWEEN {estimated_diameter_min[0]} AND {estimated_diameter_min[1]}
  AND A.estimated_diameter_max_km BETWEEN {estimated_diameter_max[0]} AND {estimated_diameter_max[1]}
  AND A.is_potentially_hazardous_asteroid = {hazardous_state}
"""

sql_query = query_map.get(selected_query)
 # Rebuild sql_query with filters
# sql_query = base_query.strip() + "\n" + filters + "\nORDER BY B.close_approach_date DESC LIMIT 1000"



video_file = open("C:/Users/HP/Downloads/NEO Animation.mp4", "rb")
video_bytes = video_file.read() 
st.video(video_bytes)
st.write("Animation of Asteroid close approach to Earth")

try:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql_query)
    result = cursor.fetchall()
    st.write (cursor.description)
    df = pd.DataFrame (result,columns = [i[0] for i in cursor.description])
    st.subheader("Query Result")
    st.dataframe(df)

except Exception as e:
    st.error(f"Failed to execute query: {e}")

finally:
    cursor.close()
    conn.close()






