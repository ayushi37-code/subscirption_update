import mysql.connector
from config import DB_CONNECTION_STR
# import methodApi2,methodApi
import ast
from config import DB_CONNECTION_STR
from collections import defaultdict

def get_connection(data):
    khost, kuser, kpasswd, kdb = data.split('#')
    conn = mysql.connector.connect(host=khost, user=kuser, passwd=kpasswd, db=kdb, charset="utf8")
    cur = conn.cursor()
    return conn, cur


# def fetch_all_document_upload_sub_fis():
#     conn, cur = get_connection(DB_CONNECTION_STR)
#     sql = "SELECT userId, upload_time, process_st_time, process_en_time, process_status, subscription FROM uploadDocuments WHERE subscription ='sub_fis';"
#     cur.execute(sql,())
#     result = cur.fetchall()
#     cur.close()
#     conn.close()
#     return result

# def fetch_all_document_upload_sub_int():
#     conn, cur = get_connection(DB_CONNECTION_STR)
#     sql = "SELECT userId, upload_time, process_st_time, process_en_time, process_status, subscription FROM uploadDocuments WHERE subscription ='sub_int';"
#     cur.execute(sql,())
#     result = cur.fetchall()
#     cur.close()
#     conn.close()
#     return result
 
# def fetch_all_document_upload_sub_srm():
#     conn, cur = get_connection(DB_CONNECTION_STR)
#     sql = "SELECT userId, upload_time, process_st_time, process_en_time, process_status, subscription FROM uploadDocuments WHERE subscription = 'sub_srm' ;"
#     cur.execute(sql,())
#     result = cur.fetchall()
#     cur.close()
#     conn.close()
#     return result

# def fetch_all_document_upload_sub_itm():
#     conn, cur = get_connection(DB_CONNECTION_STR)
#     sql = "SELECT userId, upload_time, process_st_time, process_en_time, process_status, subscription FROM uploadDocuments WHERE subscription ='sub_itm';"
#     cur.execute(sql,())
#     result = cur.fetchall()
#     cur.close()
#     conn.close()
#     return result



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



if __name__ == "__main__":
    print(fetch_all_document_upload_for_subscriptions())
   
