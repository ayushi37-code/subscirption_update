import streamlit as st
import pandas as pd
from datetime import datetime
import mysql.connector
from config import DB_CONNECTION_STR
import ast
from config import DB_CONNECTION_STR
from collections import defaultdict

def get_connection(data):
    khost, kuser, kpasswd, kdb = data.split('#')
    conn = mysql.connector.connect(host=khost, user=kuser, passwd=kpasswd, db=kdb, charset="utf8")
    cur = conn.cursor()
    return conn, cur



def fetch_all_document_upload_for_subscriptions():
    conn, cur = get_connection(DB_CONNECTION_STR)
    sql = """
    SELECT 
        subscription,
        MAX(upload_time) AS last_upload_time,
        MAX(CASE WHEN process_status = '100' THEN process_en_time ELSE NULL END) AS last_process_time,
        MAX(CASE WHEN process_status = '100' THEN userId ELSE NULL END) AS last_processed_doc_id,
        COUNT(CASE WHEN upload_time >= NOW() - INTERVAL 7 DAY THEN 1 ELSE NULL END) AS total_docs_last_7_days
    FROM 
        uploadDocuments
    WHERE 
        subscription IN ('sub_fis', 'sub_int', 'sub_srm', 'sub_itm')
    GROUP BY 
        subscription;
    """
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    # Process the results into a dictionary format
    output = []
    for row in results:
        output.append({
            "subscription": row[0],
            "last_upload_time": row[1],
            "last_process_doc_id": row[3],
            "last_process_time": row[2],
            "total_docs_last_7_days": row[4],
        })
    return output




def main():
    st.title("Live Subscription Data Dashboard")

    # Auto-refresh using the 'streamlit-autorefresh' package
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=10 * 1000, key="auto_refresh")  # Refresh every 10 seconds

    # Fetch data from the database
    data = fetch_all_document_upload_for_subscriptions()

    # Convert to Pandas DataFrame
    df = pd.DataFrame(data)

    # Format datetime columns for display
    for col in ['last_upload_time', 'last_process_time']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, datetime) else x)

    # Display the data
    st.subheader("Subscription Data")
    st.dataframe(df)

    # Manual refresh button (Optional)
    if st.button("Refresh Data"):
        st.session_state.manual_refresh = True
        st.write("Data refreshed!")

if __name__ == "__main__":
    # Reset session state for manual refresh
    if "manual_refresh" in st.session_state:
        del st.session_state["manual_refresh"]
    main()
