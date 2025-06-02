# Trento's Public Transportation: Routes and Buses Real-time Congestion Tracker

## Overview

This Streamlit application provides a comprehensive dashboard for monitoring and analyzing public transportation in Trento. The application consists of multiple pages, each serving a specific purpose in tracking and analyzing bus congestion, routes, and passenger flow.

## Features

### 1. Home Page (main.py)

- Displays the project title and description
- Provides information about the project's purpose and team
- Contains links to the project's GitHub repository
- Features a beautiful UI with Trento's imagery
- Contact information for the development team

### 2. Congestion Tracker (page1.py)

- Real-time monitoring of bus congestion levels
- Filter data by specific routes
- Displays latest congestion data in a tabular format
- Auto-refresh functionality to keep data current
- Color-coded congestion levels for easy interpretation

### 3. Live Route Visualization (page2.py)

- Interactive 3D map visualization of bus routes
- Real-time bus position tracking with animated markers
- Color-coded path segments based on congestion levels:
  - 🔴 Red: High congestion
  - 🟡 Yellow: Medium congestion
  - 🟢 Green: Low congestion
- Displays bus stops along the route with tooltips
- Detailed stop information in a separate table

### 4. Congestion Forecast (page3.py)

- Predictive analytics for future congestion levels
- Customizable forecast parameters:
  - Forecast window (15-120 minutes)
  - Prediction interval (5-15 minutes)
  - Temperature
  - Precipitation probability
  - Weather conditions
  - Traffic levels
  - Special events
- Interactive line chart showing predicted congestion levels
- Average congestion level indicators with color coding

### 5. Analytics Dashboard (page4.py)

- Comprehensive passenger flow analysis
- Interactive filters:
  - Date selection
  - Time range selection (06:00-22:00)
  - Route selection
  - Stop selection
- Visualizations:
  - Hourly passenger boarding/alighting bar charts
  - Average passenger flow trends
  - Summary statistics
- Detailed metrics for:
  - Total passengers boarding
  - Total passengers alighting

### 6. System Settings & Anomaly Detection (page5.py)

- System health monitoring
- Anomaly detection for:
  - Unusual traffic patterns
  - Sensor readings
  - Bus delays
  - Missing data
- Real-time alerts and warnings
- System status information

### 7. Prediction Service (page6.py)

- Advanced congestion prediction system
- Input parameters:
  - Route selection
  - Date and time
  - Weather conditions
  - Traffic levels
  - Special events
- Database integration for storing predictions
- Retry mechanism for robust operation
- Location-based feature consideration (schools, hospitals)

## Technical Details

### Dependencies

The application requires the following main packages:

```
streamlit
pandas
numpy
pydeck
altair
psycopg2
requests
```

### Database Configuration

The application connects to a PostgreSQL database with the following default configuration:

- Database: raw_data
- User: postgres
- Host: db
- Port: 5432

### Docker Support

The application includes a Dockerfile for containerized deployment.

## Getting Started

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

2. Run the Streamlit application:

```bash
streamlit run main.py
```

3. Access the application through your web browser at `http://localhost:8501`

## Data Sources

The application integrates with multiple data sources:

- Real-time bus location data
- Historical congestion data
- Weather data
- Traffic information
- Special events calendar
- Passenger counting systems
