import streamlit as st
import base64


# ---- PERSONAL INFO ----
NAME = "Trento's Public Transportations: Routes and Buses Real-time Congestion Tracker"
DESCRIPTION = "Data Science MSc | University Of Trento | Big Data Technology Project"
DESCRIPTION2 = """Hi! We are three students from the Data Science MSC. We designed this big data system to monitor congestion levels in public transit systems 
across the city of Trento in real-time. With this project we tried to detect bottlenecks or service disruptions, applying predictive models that can forecast 
peak usage times. The aim of this ambitious idea was to help and provide a consistent guide for transit authorities in order to implement optimal vehicle
distribution and scheduling, ultimately improving commuter experiences and reducing wait times."""
DESCRIPTION3= "If you have any questions for us or you want contribute to this project with fresh ideas, do not hesitate to send us an email!"
EMAIL1 = "luisa.porzio@studenti.unitn.it"
EMAIL2 = "virginia.dimauro@studenti.unitn.it"
EMAIL3 = "anthony.tricarico@studenti.unitn.it"
LINKEDIN = "https://www.linkedin.com/in/luisa-porzio-a1401b244/"
GITHUB = "https://github.com/LuPorzio"


# ---- LAYOUT ----
st.set_page_config(page_title="Trento's Public Transportations: Real-time Buses Congestion", layout="wide")
# Create two columns: 75% for text, 25% for image
col1, col2 = st.columns([3, 1])  # You can adjust ratio here

with col1:
    st.title("Trento's Public Transportations: Real-time Buses Congestion")
    st.subheader(DESCRIPTION)
    #st.write(DESCRIPTION2)

    # Transparent box with main description
    st.markdown(
        f"""
        <div style="background-color: rgba(0, 123, 255, 0.1); padding: 20px; border-radius: 10px;">
            <p style="color: white; font-size: 16px;">{DESCRIPTION2}</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown('<hr style="height:2px;border:none;color:#333;background-color:#333;" />', unsafe_allow_html=True)
    st.write(DESCRIPTION3)
    st.markdown(f"""
    📧 {EMAIL1} &nbsp;&nbsp;&nbsp;&nbsp; 📧 {EMAIL2} &nbsp;&nbsp;&nbsp;&nbsp; 📧 {EMAIL3}
    """, unsafe_allow_html=True)
    st.markdown(":rainbow[Take a look at our full code by clicking on the button below!]") 

with col2:
    st.image("images/Trento, monumento a Dante Alighieri (monte Bondone sullo sfondo) - L'Image Gallery.jpg", width=180)  # Use your own image

button_html = f'''
    <style>
        .button {{
            display: inline-block;
            background-color: #0066cc;
            color: white;
            padding: 0.75em 1.5em;
            border-radius: 10px;
            text-decoration: none;
            font-weight: 600;
            margin: 10px;
        }}
        .button:hover {{
            background-color: #004d99;
        }}
    </style>
'''

# st.markdown(button_html, unsafe_allow_html=True)
st.image('images/Screenshot 2025-05-19 at 14.07.06.png', width=1000)