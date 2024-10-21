import pandas as pd
import streamlit as st
import snowflake.connector
from datetime import datetime, timedelta
from io import BytesIO
from decimal import Decimal
from tqdm import tqdm
from stqdm import stqdm  # Import stqdm for Streamlit integration
from PIL import Image

# ===== Connect & Fetch Database =====
# Access secrets using st.secrets
user = st.secrets["snowflake"]["user"]
password = st.secrets["snowflake"]["password"]
account = st.secrets["snowflake"]["account"]
warehouse = st.secrets["snowflake"]["warehouse"]
database = st.secrets["snowflake"]["database"]
schema = st.secrets["snowflake"]["schema"]

# Establish connection to Snowflake
connection = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# ===== Streamlit Input Widgets =====
st.title("Shipping Performance Weekly Report")

# Input StartDate and EndDate
start_date = st.date_input("Pilih Tanggal Mulai", value=datetime.now() - timedelta(days=30))
end_date = st.date_input("Pilih Tanggal Akhir", value=datetime.now())

# Validate the date inputs
def validate_date_inputs(start_date, end_date):
    errors = []
    if start_date > end_date:
        errors.append("Tanggal mulai tidak boleh lebih besar dari tanggal akhir.")
    return errors

# Function to calculate GMV EOM
def calculate_gmv_eom(gmv_final, days, days_in_period):
    gmv_eom = float(gmv_final) / days * days_in_period
    return gmv_eom

# Tombol Submit
if st.button('Submit'):
    errors = validate_date_inputs(start_date, end_date)

    if errors:
        for error in errors:
            st.error(error)
    else:
        start_timestamp = int(datetime.combine(start_date, datetime.min.time()).timestamp())
        end_timestamp = int(datetime.combine(end_date, datetime.max.time()).timestamp())
        days_in_period = (end_date - start_date).days + 1

        # Perhitungan preStart dan preEnd
        c = end_timestamp - start_timestamp
        preStart_timestamp = start_timestamp - c
        preEnd_timestamp = end_timestamp - c

        # Query to fetch data from Snowflake
        query = f"""
        SELECT
            COALESCE(SUM(CASE WHEN so.status IN (500, 702, 703) THEN so.gmv_shipment END), 0) AS gmv_final_status,
            COUNT(CASE WHEN so.status >= 300 AND so.status < 500 THEN 1 END) AS order_qty,
            COUNT(DISTINCT(CASE WHEN EXISTS (
                SELECT 1 FROM shipment_orders so3
                WHERE so3.created_by = so.created_by
                  AND so3.created_at < {start_timestamp}
            ) THEN so.created_by END)) AS r_trx_user,
            COUNT(DISTINCT(CASE WHEN NOT EXISTS (
                SELECT 1 FROM shipment_orders so2
                WHERE so2.created_by = so.created_by
                  AND so2.created_at >= {preStart_timestamp} AND so2.created_at <= {preEnd_timestamp}
            ) THEN so.created_by END)) AS n_trx_user,
            (SELECT COUNT(DISTINCT ul.user_id)
             FROM user_logs ul
             WHERE ul.created_at >= {start_timestamp}
               AND ul.created_at <= {end_timestamp}) AS active_user,
            COUNT(DISTINCT so.created_by) AS trx_user,
            AVG(so.transaction_value) AS aov,
            CASE
                WHEN COUNT(CASE WHEN so.status IN (500, 703) THEN 1 END) = 0
                THEN 0
                ELSE (COUNT(CASE WHEN so.status = 702 THEN 1 END)::NUMERIC / 
                      COUNT(CASE WHEN so.status IN (500, 703) THEN 1 END)::NUMERIC)
            END AS cod_rts
        FROM shipment_orders so
        WHERE so.created_at >= {start_timestamp} AND so.created_at <= {end_timestamp};
        """

        cur = connection.cursor()
        cur.execute(query)
        result = cur.fetchone()

        # Prepare DataFrame with results
        data = {
            'GMV Final Status': [float(result[0])],
            'Orders Qty': [result[1]],
            'R Transacting User': [result[2]],
            'N Transacting User': [result[3]],
            'AU (Aktive User)': [result[4]],
            'TU (Trx User)': [result[2] + result[3]],
            'AOV': [result[6]],
            'COD RTS%': [result[7]]
        }

        df = pd.DataFrame(data)

        # Display the DataFrame
        st.dataframe(df, use_container_width=True)

        # Download the DataFrame as Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        processed_data = output.getvalue()

        file_name = f"report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"

        st.download_button(label='Download as Excel',
                           data=processed_data,
                           file_name=file_name,
                           mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
