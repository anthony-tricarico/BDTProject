import streamlit as st
from datetime import datetime
import time

# Define the pages
main = st.Page("main.py", title="Public Transport Congestion", icon="🚌")
page1 = st.Page("page1.py", title="Real-Time Congestion Tracker", icon="🚨")
page2 = st.Page("page2.py", title="Map", icon="🗺️")
page3 = st.Page("page3.py", title="Forecast", icon="🕟")
page4 = st.Page("page4.py", title="Analytics", icon="📊")
page5 = st.Page("page5.py", title="Anomalies", icon="⚠️")
page6 = st.Page("page6.py", title="Prediction", icon="🔮")

# Set up navigation
pgl = st.navigation([main, page1, page2, page3, page4, page5, page6])

# Run the selected page
pgl.run()