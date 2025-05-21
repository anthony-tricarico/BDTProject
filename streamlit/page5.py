import streamlit as st

st.title("settings")

#st.markdown("# Page 1 ❄️")

#Sidebar settings
st.sidebar.markdown("settings")

st.subheader("⚠️ Anomaly Detection")
st.write("Unusual traffic patterns or sensor readings.")
st.dataframe({
    "Stop": ["A", "B"],
    "Anomaly Score": [0.92, 0.85],
    "Time": ["08:05", "08:17"]
})

st.subheader("⚠️ Anomaly Detection")
st.warning("Bus #45 is running 20 minutes late on Route A.")
st.warning("Sensor data missing from Stop 'Via Verdi'.")
st.info("No major anomalies in the past hour.")