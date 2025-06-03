import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import pydeck as pdk
import psycopg2
from datetime import datetime, timedelta

st.title("Analytics")
st.info("This page shows information on passengers flow throughout the day, based on the selected route and selected stop.")

# Database connection function
def get_db_connection():
    return psycopg2.connect(
        dbname="raw_data",
        user="postgres",
        password="example",
        host="db",
        port="5432"
    )

# Date filter
selected_date = st.date_input("Select Date", datetime.now().date() + timedelta(days=1))

# Time range filter using slider
time_range = st.slider(
    "Select Time Range",
    min_value=datetime.strptime("06:00", "%H:%M").time(),
    max_value=datetime.strptime("22:00", "%H:%M").time(),
    value=(datetime.strptime("06:00", "%H:%M").time(), datetime.strptime("22:00", "%H:%M").time()),
    format="HH:mm",
    step=timedelta(minutes=60)
)

# Route and stop filters
try:
    conn = get_db_connection()
    
    # Get unique routes
    routes_query = "SELECT DISTINCT route FROM raw_tickets ORDER BY route"
    routes_df = pd.read_sql_query(routes_query, conn)
    selected_route = st.selectbox("Select Route", ["All"] + routes_df['route'].tolist())
    
    # Get unique stops
    stops_query = "SELECT DISTINCT stop_id FROM raw_tickets ORDER BY stop_id"
    stops_df = pd.read_sql_query(stops_query, conn)
    selected_stop = st.selectbox("Select Stop", ["All"] + stops_df['stop_id'].tolist())
    
    # Build the query for passengers getting on (from tickets)
    boarding_query = """
    SELECT 
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as passengers_boarding
    FROM raw_tickets
    WHERE DATE(timestamp) = %s
    AND timestamp::time BETWEEN %s AND %s
    """
    
    # Build the query for passengers getting off (from sensors)
    alighting_query = """
    SELECT 
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as passengers_alighting
    FROM raw_sensors
    WHERE DATE(timestamp) = %s
    AND timestamp::time BETWEEN %s AND %s
    AND status = 1
    """
    
    # Add route and stop filters if not 'All'
    params_boarding = [selected_date, time_range[0], time_range[1]]
    params_alighting = [selected_date, time_range[0], time_range[1]]
    
    if selected_route != "All":
        boarding_query += " AND route = %s"
        alighting_query += " AND route = %s"
        params_boarding.append(selected_route)
        params_alighting.append(selected_route)
    
    if selected_stop != "All":
        boarding_query += " AND stop_id = %s"
        alighting_query += " AND stop_id = %s"
        params_boarding.append(selected_stop)
        params_alighting.append(selected_stop)
    
    boarding_query += " GROUP BY hour ORDER BY hour"
    alighting_query += " GROUP BY hour ORDER BY hour"
    
    # Execute queries
    boarding_df = pd.read_sql_query(boarding_query, conn, params=params_boarding)
    alighting_df = pd.read_sql_query(alighting_query, conn, params=params_alighting)
    
    # Merge the dataframes
    boarding_df['hour'] = pd.to_datetime(boarding_df['hour'])
    alighting_df['hour'] = pd.to_datetime(alighting_df['hour'])
    
    # Create a complete range of hours
    hour_range = pd.date_range(
        start=f"{selected_date} {time_range[0]}",
        end=f"{selected_date} {time_range[1]}",
        freq='H'
    )
    
    # Create the final dataframe with all hours
    final_df = pd.DataFrame({'hour': hour_range})
    final_df = final_df.merge(boarding_df, on='hour', how='left')
    final_df = final_df.merge(alighting_df, on='hour', how='left')
    final_df = final_df.fillna(0)
    
    # Prepare data for visualization
    chart_data = pd.melt(
        final_df,
        id_vars=['hour'],
        value_vars=['passengers_boarding', 'passengers_alighting'],
        var_name='type',
        value_name='passengers'
    )
    
    # Create a more readable mapping for the legend
    chart_data['type'] = chart_data['type'].map({
        'passengers_boarding': 'Boarding',
        'passengers_alighting': 'Alighting'
    })
    
    # Create the visualization
    st.subheader("Hourly Passenger Flow")
    
    # Base chart with selection
    selection = alt.selection_multi(fields=['type'], bind='legend')
    
    # Create the bar chart with enhanced styling
    chart = alt.Chart(chart_data).mark_bar(
        cornerRadius=2,
        width=25  # Make bars wider
    ).encode(
        x=alt.X('hour:T', 
                title='Hour', 
                axis=alt.Axis(
                    format='%H:%M',
                    labelAngle=-45
                ),
                scale=alt.Scale(padding=0.2)),  # Add padding between bars
        y=alt.Y('passengers:Q', 
                title='Number of Passengers',
                scale=alt.Scale(zero=True)),
        color=alt.Color('type:N', 
                       scale=alt.Scale(domain=['Boarding', 'Alighting'],
                                     range=['#2ecc71', '#e74c3c']),
                       legend=alt.Legend(
                           title='Passenger Type',
                           orient='top'
                       )),
        opacity=alt.condition(selection, alt.value(0.9), alt.value(0.2)),
        tooltip=[
            alt.Tooltip('hour:T', title='Time', format='%H:%M'),
            alt.Tooltip('passengers:Q', title='Passengers', format=',d'),
            alt.Tooltip('type:N', title='Type')
        ]
    ).properties(
        height=400,
        width=700  # Set a fixed width for better proportions
    ).add_selection(
        selection
    )
    
    # Add a rule to show average line
    rule = alt.Chart(chart_data).mark_rule(
        strokeDash=[12, 6],
        strokeWidth=2,
        opacity=0.5
    ).encode(
        y='mean(passengers):Q',
        color=alt.Color('type:N',
                       scale=alt.Scale(domain=['Boarding', 'Alighting'],
                                     range=['#2ecc71', '#e74c3c'])),
        size=alt.value(2)
    ).transform_filter(
        selection
    )
    
    # Combine the chart with the rule
    final_chart = (chart + rule).configure_view(
        strokeWidth=0
    ).configure_axis(
        grid=True,
        gridOpacity=0.2,
        domainOpacity=0.8,
        ticks=True,
        tickOffset=5,
        tickWidth=2,
        labelColor='#FAFAFA',  # Light text color for axis labels
        titleColor='#FAFAFA'   # Light text color for axis titles
    ).configure_legend(
        strokeColor='#262730',
        fillColor='#262730',   # Dark background matching the theme
        padding=10,
        cornerRadius=5,
        orient='top',
        labelColor='#FAFAFA',  # Light text color for legend labels
        titleColor='#FAFAFA'   # Light text color for legend title
    ).configure_title(
        color='#FAFAFA'        # Light text color for chart title
    )
    
    st.altair_chart(final_chart, use_container_width=True)
    
    # Display summary statistics
    st.subheader("Summary Statistics")
    total_boarding = final_df['passengers_boarding'].sum()
    total_alighting = final_df['passengers_alighting'].sum()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Passengers Boarding", f"{int(total_boarding):,}")
    with col2:
        st.metric("Total Passengers Alighting", f"{int(total_alighting):,}")
    
except Exception as e:
    st.error(f"Error connecting to database: {str(e)}")
    st.info("Please configure your database connection parameters in the get_db_connection() function.")
finally:
    if 'conn' in locals():
        conn.close()