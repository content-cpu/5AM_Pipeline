import os, time, requests, re, logging, csv, smtplib
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
# 1. SECURE SETTINGS (Read from Google Cloud)
# ==========================================
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

# *** MAJOR CHANGE: DATE IS NOW YESTERDAY ***
TARGET_DATE = date.today() - timedelta(days=1)

SEARCH_WINDOW_HOURS = 48
MAX_WORKERS = 5
API_TIMEOUT = 20

# File saves to /tmp/ for Cloud compatibility, names it with YESTERDAY'S date
OUTPUT_FILENAME = f"/tmp/NEWS_{TARGET_DATE.strftime('%d_%b')}_{SEARCH_WINDOW_HOURS}HR.csv"

# FULL COMPANY LIST
COMPANIES =[
    "Reliance Industries", "RELIANCE", "HDFC Bank", "HDFCBANK",
    "Tata Consultancy Services", "TCS", "Infosys", "INFY", "ICICI Bank", "ICICIBANK",
    "Hindustan Unilever", "HINDUNILVR", "State Bank of India", "SBIN", "Bharti Airtel",
    "BHARTIARTL", "ITC", "Larsen & Toubro", "LT", "Kotak Bank", "KOTAKBANK",
    "Axis Bank", "AXISBANK", "Maruti Suzuki India", "MARUTI", "Sun Pharmaceutical Industries",
    "SUNPHARMA", "Titan Company", "TITAN", "HCL Technologies", "HCLTECH", "Bajaj Finance",
    "BAJFINANCE", "Asian Paints", "ASIANPAINT", "NTPC", "Wipro", "WIPRO",
    "Power Grid Corporation of India", "POWERGRID", "UltraTech Cement", "ULTRACEMCO",
    "Tata Motors", "TATAMOTORS", "Oil & Natural Gas Corporation", "ONGC", "JSW Steel",
    "JSWSTEEL", "Coal India", "COALINDIA", "Bharat Petroleum Corporation", "BPCL",
    "Adani Ports and Special Economic Zone", "ADANIPORTS", "Hindalco Industries", "HINDALCO",
    "Grasim Industries", "GRASIM", "Tech Mahindra", "TECHM", "Eicher Motors", "EICHERMOT",
    "Bajaj Finserv", "BAJAJFINSV", "Nestle India", "NESTLEIND", "Trent", "TRENT",
    "SBI Life Insurance Company", "SBILIFE", "Vedanta", "VEDL", "Adani Enterprises",
    "ADANIENT", "Shriram Finance", "SHRIRAMFIN", "Zomato", "ZOMATO", "Bharat Electronics",
    "BEL", "Hindustan Aeronautics", "HAL", "IndusInd Bank", "INDUSINDBK", "Bajaj Auto",
    "BAJAJ-AUTO", "Dr. Reddy's Laboratories", "DRREDDY", "Cipla", "CIPLA", "Tata Steel",
    "TATASTEEL", "Apollo Hospitals Enterprise", "APOLLOHOSP", "LTIMindtree", "LTIM",
    "Divi's Laboratories", "DIVISLAB", "Eternal", "ETERNAL",
    "Life Insurance Corporation of India", "LICI", "Mahindra & Mahindra", "M&M",
    "Adani Power", "ADANIPOWER", "Avenue Supermarts", "DMART", "Indian Oil Corporation",
    "IOC", "InterGlobe Aviation", "INDIGO", "Hindustan Zinc", "HINDZINC",
    "Hyundai Motor India", "HYUNDAI", "Jio Financial Services", "JIOFIN", "DLF",
    "Adani Green Energy", "ADANIGREEN", "TVS Motor Company", "TVSMOTOR",
    "HDFC Life Insurance Company", "HDFCLIFE", "Indian Railway Finance Corporation", "IRFC",
    "Varun Beverages", "VBL", "Pidilite Industries", "PIDILITIND", "Trent Motors Private",
    "TMPV", "Britannia Industries", "BRITANNIA", "Bank of Baroda", "BANKBARODA",
    "Ambuja Cements", "AMBUJACEM", "Bajaj Holdings & Investment", "BAJAJHLDNG",
    "Cholamandalam Investment and Finance Company", "CHOLAFIN", "Tata Capital", "TATACAP",
    "Punjab National Bank", "PNB", "Power Finance Corporation", "PFC", "Muthoot Finance",
    "MUTHOOTFIN", "Tata Power Company", "TATAPOWER", "Solar Industries India", "SOLARINDS",
    "Torrent Pharmaceuticals", "TORNTPHARM", "Macrotech Developers", "LODHA",
    "HDFC Asset Management Company", "HDFCAMC", "Canara Bank", "CANBK", "GAIL (India)",
    "GAIL", "Godrej Consumer Products", "GODREJCP", "CG Power and Industrial Solutions",
    "CGPOWER", "Energy India", "ENRIN", "Cummins India", "CUMMINSIND",
    "Tata Consumer Products", "TATACONSUM", "Polycab India", "POLYCAB",
    "Mazagon Dock Shipbuilders", "MAZDOCK", "Bosch", "BOSCHLTD", "LG Electronics India",
    "LGEINDIA", "Adani Energy Solutions", "ADANIENSOL", "Max Healthcare Institute",
    "MAXHEALTH", "Samvardhana Motherson International", "MOTHERSON", "Siemens", "SIEMENS",
    "Union Bank of India", "UNIONBANK", "ABB India", "ABB", "Indian Bank", "INDIANB",
    "Hero MotoCorp", "HEROMOTOCO", "Jindal Steel & Power", "JINDALSTEL", "IDBI Bank", "IDBI"
]

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def create_retry_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"})
    return session

def fetch_company_news(session, rank, stock):
    end_date = TARGET_DATE + timedelta(hours=SEARCH_WINDOW_HOURS)
    payload = {
        "q": f'"{stock}" OR "{stock}.NS" after:{TARGET_DATE} before:{end_date}', 
        "num": 20, 
        "gl": "in", 
        "hl": "en"
    }
    try:
        response = session.post("https://google.serper.dev/news", json=payload, timeout=API_TIMEOUT)
        if response.status_code == 200: 
            return rank, stock, response.json().get("news",[])
    except Exception as e: 
        print(f"Error on {stock}: {e}")
    return rank, stock, None

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def main():
    print(f"Starting pipeline for Target Date: {TARGET_DATE}")
    all_results =[]
    session = create_retry_session()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {executor.submit(fetch_company_news, session, rank, stock): stock for rank, stock in enumerate(COMPANIES, 1)}
        for future in as_completed(future_to_stock):
            rank, stock, news_items = future.result()
            if not news_items: continue
            for item in news_items:
                title = item.get("title", "").strip()
                if title: 
                    all_results.append([rank, stock, title, item.get("source", ""), item.get("link", "")])

    # 1. Save locally to /tmp/
    all_results.sort(key=lambda x: x[0])
    with open(OUTPUT_FILENAME, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["RANK", "STOCK", "NEWS HEADLINE", "SOURCE", "LINK"])
        writer.writerows(all_results)
    
    # 2. Upload to Google Cloud Storage
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        cloud_filename = f"raw_news/NEWS_{TARGET_DATE.strftime('%d_%b')}.csv"
        bucket.blob(cloud_filename).upload_from_filename(OUTPUT_FILENAME)
        print(f"Successfully uploaded to GCS: {cloud_filename}")
    except Exception as e:
        print(f"GCS Upload Failed: {e}")
    
    # 3. Send Email
    try:
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
        print("Successfully sent email to Manager.")
    except Exception as e:
        print(f"Email Send Failed: {e}")

if __name__ == "__main__":
    main()
