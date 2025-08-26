from flask import Flask, render_template, request, jsonify
import pandas as pd
import os
from geopy.geocoders import Nominatim
from time import sleep
import requests

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

DATASTORE = {}  # store last uploaded dataset in memory

# --- Detect relevant columns ---
def detect_columns(df):
    lat_col = next((c for c in df.columns if 'lat' in c.lower()), None)
    lon_col = next((c for c in df.columns if 'lon' in c.lower()), None)
    measure_col = next((c for c in df.columns if 'amount' in c.lower() or 'sales' in c.lower()), None)
    date_col = next((c for c in df.columns if 'date' in c.lower()), None)
    location_col = next((c for c in df.columns if 'location' in c.lower() or 'address' in c.lower()), None)
    hover_col = next((c for c in df.columns if 'store' in c.lower() or location_col), df.columns[0])
    return lat_col, lon_col, measure_col, date_col, location_col, hover_col

def geocode_missing(df, location_col):
    geolocator = Nominatim(user_agent="geo_analytics_app")
    latitudes, longitudes = [], []
    for _, row in df.iterrows():
        if pd.notna(row.get(location_col)):
            try:
                location = geolocator.geocode(row[location_col])
                sleep(1)
                if location:
                    latitudes.append(location.latitude)
                    longitudes.append(location.longitude)
                else:
                    latitudes.append(None)
                    longitudes.append(None)
            except:
                latitudes.append(None)
                longitudes.append(None)
        else:
            latitudes.append(None)
            longitudes.append(None)
    df["latitude"] = latitudes
    df["longitude"] = longitudes
    return df

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload():
    file = request.files["file"]
    if not file:
        return jsonify({"error": "No file uploaded"})
    
    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(filepath)

    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(filepath)
    elif ext in [".xls", ".xlsx"]:
        df = pd.read_excel(filepath)
    else:
        return jsonify({"error": "Unsupported file type"})
    
    lat_col, lon_col, measure_col, date_col, location_col, hover_col = detect_columns(df)

    if not (lat_col and lon_col) and location_col:
        df = geocode_missing(df, location_col)
        lat_col, lon_col = "latitude", "longitude"

    DATASTORE["df"] = df
    DATASTORE["measure_col"] = measure_col
    DATASTORE["date_col"] = date_col
    DATASTORE["hover_col"] = hover_col

    records = df.to_dict(orient="records")
    return jsonify({
        "columns": df.columns.tolist(),
        "lat_col": lat_col,
        "lon_col": lon_col,
        "measure_col": measure_col,
        "date_col": date_col,
        "hover_col": hover_col,
        "data": records
    })

@app.route("/insights", methods=["POST"])
def insights():
    api_key = request.json.get("api_key")
    if not api_key:
        return jsonify({"error": "Missing API key"}), 400

    df = DATASTORE.get("df")
    measure_col = DATASTORE.get("measure_col")
    date_col = DATASTORE.get("date_col")

    if df is None or measure_col is None:
        return jsonify({"error": "No dataset available"}), 400

    # Create summary for prompt
    total_sales = df[measure_col].sum()
    avg_sales = df[measure_col].mean()
    city_summary = df.groupby("latitude")[measure_col].mean().to_dict()

    summary_text = "\n".join([f"- {city}: {val:.2f}" for city, val in city_summary.items()])

    prompt = f"""
    You are a business strategist. Analyze this dataset.
    Total sales: {total_sales:.2f}
    Average sales: {avg_sales:.2f}
    City summary:
    {summary_text}

    Provide 3 actionable business insights in bullet points.
    """

    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={api_key}"
    payload = { "contents": [{ "parts": [{ "text": prompt }] }] }

    try:
        resp = requests.post(api_url, json=payload)
        resp.raise_for_status()
        result = resp.json()
        insights = result["candidates"][0]["content"]["parts"][0]["text"]
        return jsonify({"insights": insights})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
