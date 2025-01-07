import streamlit as st
import pandas as pd
from datetime import datetime
from dbApi2 import fetch_all_document_upload_for_subscriptions  # Replace with actual import

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
