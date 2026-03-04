import os
import time
import requests
import re
import logging
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import date, timedelta, timezone
from dateutil import parser
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import storage

# ==========================================
# 1. CONFIGURATION
# ==========================================
SERPER_API_KEY = "ef08ec9dba860a167166861687f5b0ae83f0dac6"
SEARCH_WINDOW_HOURS = 48
MAX_WORKERS = 5
API_TIMEOUT = 20

# NEW: Cloud & Email Settings
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
# DYNAMIC DATE: Updated to yesterday
TARGET_DATE = date.today() - timedelta(days=1)
IST_OFFSET = timedelta(hours=5, minutes=30)

# CLOUD REQUIREMENT: Must save to /tmp/ folder on cloud servers
OUTPUT_FILENAME = f"/tmp/NEWS_{TARGET_DATE.strftime('%d_%b')}_{SEARCH_WINDOW_HOURS}HR.csv"

COMPANIES =["Reliance Industries", "RELIANCE", "HDFC Bank", "HDFCBANK", "Tata Consultancy Services", "TCS"] # Add the rest of your companies back here!

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def create_retry_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["POST"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"})
    return session

def fetch_company_news(session, rank, stock):
    end_date = TARGET_DATE + timedelta(hours=SEARCH_WINDOW_HOURS)
    payload = {"q": f'"{stock}" OR "{stock}.NS" after:{TARGET_DATE} before:{end_date}', "num": 20, "gl": "in", "hl": "en"}
    try:
        response = session.post("https://google.serper.dev/news", json=payload, timeout=API_TIMEOUT)
        if response.status_code == 200: return rank, stock, response.json().get("news",[])
    except Exception: pass
    return rank, stock, None

def upload_to_gcs():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    cloud_file_name = f"raw_news/NEWS_{TARGET_DATE.strftime('%d_%b')}.csv"
    blob = bucket.blob(cloud_file_name)
    blob.upload_from_filename(OUTPUT_FILENAME)
    print(f"Uploaded to Google Cloud Storage: {cloud_file_name}")

def send_email():
    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = f"Automated Daily News Report - {TARGET_DATE.strftime('%d %b')}"

    with open(OUTPUT_FILENAME, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename= {os.path.basename(OUTPUT_FILENAME)}")
    msg.attach(part)

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(EMAIL_SENDER, EMAIL_PASSWORD)
    server.send_message(msg)
    server.quit()
    print("Email Sent Successfully!")

# ==========================================
# 3. CLOUD ENTRY POINT (Must be named 'main' with a 'request' parameter)
# ==========================================
def main(request=None):
    all_results =[]
    session = create_retry_session()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {executor.submit(fetch_company_news, session, rank, stock): stock for rank, stock in enumerate(COMPANIES, 1)}
        for future in as_completed(future_to_stock):
            rank, stock, news_items = future.result()
            if not news_items: continue
            for item in news_items:
                title = item.get("title", "").strip()
                if title: all_results.append([rank, stock, title, item.get("source", ""), item.get("link", "")])

    with open(OUTPUT_FILENAME, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["RANK", "STOCK", "NEWS HEADLINE", "SOURCE", "LINK"])
        writer.writerows(all_results)

    upload_to_gcs()
    send_email()
    return "Success!"

# For local testing
if __name__ == "__main__":
    main()
