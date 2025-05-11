google_api_key = str(input("Paste your Google API key here: "))
with open('kafka-components/kafka-producer-traffic/.env', mode='w') as f:
    f.write(f"GOOGLE_API_KEY='{google_api_key}'")