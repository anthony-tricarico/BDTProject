from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
from page1 import main as page1_main
from page2 import main as page2_main
from page3 import main as page3_main
from page4 import main as page4_main
from page5 import main as page5_main
from page6 import main as page6_main

"""
# Welcome to Streamlit!

Edit `/streamlit_app.py` to customize this app to your heart's desire :heart:

If you have any questions, checkout our [documentation](https://docs.streamlit.io) and [community
forums](https://discuss.streamlit.io).

In the meantime, below is an example of what you can do with just a few lines of code:
"""

with st.echo(code_location='below'):
   total_points = st.slider("Number of points in spiral", 1, 5000, 2000)
   num_turns = st.slider("Number of turns in spiral", 1, 100, 9)

   Point = namedtuple('Point', 'x y')
   data = []

   points_per_turn = total_points / num_turns

   for curr_point_num in range(total_points):
      curr_turn, i = divmod(curr_point_num, points_per_turn)
      angle = (curr_turn + 1) * 2 * math.pi * i / points_per_turn
      radius = curr_point_num / total_points
      x = radius * math.cos(angle)
      y = radius * math.sin(angle)
      data.append(Point(x, y))

   st.altair_chart(alt.Chart(pd.DataFrame(data), height=500, width=500)
      .mark_circle(color='#0068c9', opacity=0.5)
      .encode(x='x:Q', y='y:Q'))

st.set_page_config(
    page_title="Bus Data Analysis",
    page_icon="🚌",
    layout="wide"
)

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ["Home", "Page 1", "Page 2", "Page 3", "Page 4", "Page 5", "Prediction"]
)

# Main content
if page == "Home":
    st.title("🚌 Bus Data Analysis Dashboard")
    st.write("""
    Welcome to the Bus Data Analysis Dashboard! This application provides various insights and analysis tools for bus transportation data.
    
    Use the sidebar to navigate between different pages:
    - **Page 1**: [Description of Page 1]
    - **Page 2**: [Description of Page 2]
    - **Page 3**: [Description of Page 3]
    - **Page 4**: [Description of Page 4]
    - **Page 5**: [Description of Page 5]
    - **Prediction**: Make congestion predictions for bus trips
    """)
elif page == "Page 1":
    page1_main()
elif page == "Page 2":
    page2_main()
elif page == "Page 3":
    page3_main()
elif page == "Page 4":
    page4_main()
elif page == "Page 5":
    page5_main()
elif page == "Prediction":
    page6_main()