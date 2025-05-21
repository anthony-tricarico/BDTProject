import streamlit as st
from datetime import datetime
import time

# Define the pages
main = st.Page("main.py", title="Public Transport Congestion", icon="✉️")
page1 = st.Page("page1.py", title="Congestion Tracker", icon="🧩")
page2 = st.Page("page2.py", title="Map", icon="🗺️")
page3 = st.Page("page3.py", title="Forecast", icon="🕟")
page4 = st.Page("page4.py", title="Analytics", icon="📊")
page5 = st.Page("page5.py", title="Anomalies", icon="⚠️")

# Set up navigation
pgl = st.navigation([main, page1, page2, page3, page4, page5])

# Run the selected page
pgl.run()

# import streamlit as st
# from modules import congestion_map, forecast_panel, anomaly_panel, analytics

# st.set_page_config(layout="wide", page_title="Trento Real-Time Bus Congestion Tracker")

# st.sidebar.title("Navigation")
# page = st.sidebar.radio("Go to", ["Live Map", "Forecast", "Anomalies", "Analytics", "Settings"])

# if page == "Live Map":
#     congestion_map.display()

# elif page == "Forecast":
#     forecast_panel.display()

# elif page == "Anomalies":
#     anomaly_panel.display()

# elif page == "Analytics":
#     analytics.display()

# elif page == "Settings":
#     st.write("📊 Settings & App Configuration (Coming Soon)")
