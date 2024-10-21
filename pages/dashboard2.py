import pandas as pd
import streamlit as st
import snowflake.connector
from datetime import datetime, timedelta
from io import BytesIO
import numpy as np
from decimal import Decimal
from tqdm import tqdm
from streamlit.runtime.scriptrunner import add_script_run_ctx
from stqdm import stqdm  # Import stqdm for Streamlit integration
from PIL import Image
import plotly.express as px

# ===== Set Page =====
icon_image = Image.open("orderfaz.jpeg")
st.set_page_config(page_title="Orderfaz - Weekly Report", page_icon=icon_image, layout="wide")

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
st.write("# Orderfaz Shipping Performance Report")

# Input bulan dan tahun
month_input = st.selectbox("Pilih Bulan", range(1, 13), format_func=lambda x: datetime(1900, x, 1).strftime('%B'))
year_input = st.number_input("Pilih Tahun", min_value=2000, max_value=2100, value=datetime.now().year)

previous_month = (datetime(year_input, month_input, 1) - timedelta(days=1)).month
previous_year = (datetime(year_input, month_input, 1) - timedelta(days=1)).year


# Validasi input
def validate_inputs(month_input, year_input):
    errors = []
    if month_input < 1 or month_input > 12:
        errors.append("Bulan harus antara 1 dan 12.")
    if year_input < 2000 or year_input > 2100:
        errors.append("Tahun harus antara 2000 dan 2100.")
    return errors


# Function to calculate GMV EOM
def calculate_gmv_eom(gmv_final, days, days_in_month):
    gmv_eom = float(gmv_final) / days * days_in_month
    return gmv_eom


# Function to generate weekly data based on month and year
def generate_weeks(month, year):
    start_date = datetime(year, month, 1)
    end_date = (datetime(year, month + 1, 1) - timedelta(seconds=1)) if month < 12 else datetime(year, month, 31, 23, 59, 59)
    days_in_month = (end_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    days_in_month = days_in_month.day
    data = []
    day_of_week = start_date.weekday()

    if day_of_week == 0:
        current_date = start_date
    elif day_of_week in {1, 2, 3, 4}:
        end_of_week = start_date + timedelta(days=(6 - day_of_week))
        end_of_week = end_of_week.replace(hour=23, minute=59, second=59)
        data.append({
            "Tanggal Senin (Awal Minggu)": start_date.strftime('%Y-%m-%d %H:%M:%S'),
            "Tanggal Minggu (Akhir Minggu)": end_of_week.strftime('%Y-%m-%d %H:%M:%S'),
            "Minggu ke-": 1,
            "Bulan": start_date.strftime('%B')
        })
        current_date = end_of_week + timedelta(seconds=1)
    else:
        start_of_week = start_date
        end_of_week = start_of_week + timedelta(days=(6 - day_of_week + 7))
        end_of_week = end_of_week.replace(hour=23, minute=59, second=59)
        data.append({
            "Tanggal Senin (Awal Minggu)": start_of_week.strftime('%Y-%m-%d %H:%M:%S'),
            "Tanggal Minggu (Akhir Minggu)": end_of_week.strftime('%Y-%m-%d %H:%M:%S'),
            "Minggu ke-": 1,
            "Bulan": start_of_week.strftime('%B')
        })
        current_date = end_of_week + timedelta(seconds=1)

    week_number = 2 if day_of_week != 0 else 1

    while current_date <= end_date:
        start_of_week = current_date
        end_of_week = start_of_week + timedelta(days=6)
        end_of_week = end_of_week.replace(hour=23, minute=59, second=59)

        if end_of_week > end_date:
            end_of_week = end_date

        data.append({
            "Tanggal Senin (Awal Minggu)": start_of_week.strftime('%Y-%m-%d %H:%M:%S'),
            "Tanggal Minggu (Akhir Minggu)": end_of_week.strftime('%Y-%m-%d %H:%M:%S'),
            "Minggu ke-": week_number,
            "Bulan": start_of_week.strftime('%B')
        })

        current_date = end_of_week + timedelta(seconds=1)
        week_number += 1

    if len(data) > 1 and (datetime.strptime(data[-1]['Tanggal Minggu (Akhir Minggu)'], '%Y-%m-%d %H:%M:%S') - datetime.strptime(data[-1]['Tanggal Senin (Awal Minggu)'], '%Y-%m-%d %H:%M:%S')).days < 6:
        data[-2]['Tanggal Minggu (Akhir Minggu)'] = data[-1]['Tanggal Minggu (Akhir Minggu)']
        data.pop()

    return pd.DataFrame(data), days_in_month


# Tombol Submit
if st.button('Submit'):
    errors = validate_inputs(month_input, year_input)

    if errors:
        for error in errors:
            st.error(error)
    else:
        weeks_df, days_in_month = generate_weeks(month_input, year_input)

        # Tambahkan kolom GMV EOM setelah GMV Final Status
        weeks_df[['GMV Final Status', 'GMV EOM', 'Orders Qty', 'R Transacting User', 'N Transacting User', 'AU (Aktive User)', 'TU (Trx User)', 'AOV', 'COD RTS%']] = None

        cumulative_gmv = Decimal(0)

        # Menggunakan stqdm untuk progress bar di Streamlit
        for i, row in stqdm(weeks_df.iterrows(), total=weeks_df.shape[0], desc="Processing Weeks"):
            st.write(f"Processing week {i + 1}")
            start_date = datetime.strptime(row['Tanggal Senin (Awal Minggu)'], '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(row['Tanggal Minggu (Akhir Minggu)'], '%Y-%m-%d %H:%M:%S')

            start_timestamp = int(start_date.timestamp())
            end_timestamp = int(end_date.timestamp())

            # Perhitungan preStart dan preEnd
            c = end_timestamp - start_timestamp
            preStart_timestamp = start_timestamp - c
            preEnd_timestamp = end_timestamp - c

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

            # Update DataFrame with query results
            weeks_df.at[i, 'GMV Final Status'] = float(result[0])  # Convert Decimal to float
            weeks_df.at[i, 'Orders Qty'] = result[1]
            weeks_df.at[i, 'R Transacting User'] = result[2]
            weeks_df.at[i, 'N Transacting User'] = result[3]
            weeks_df.at[i, 'AU (Aktive User)'] = result[4]
            weeks_df.at[i, 'TU (Trx User)'] = result[2] + result[3]
            weeks_df.at[i, 'AOV'] = result[6]
            weeks_df.at[i, 'COD RTS%'] = result[7]

            # Calculate cumulative GMV up to the current week
            cumulative_gmv += result[0]

            # Calculate GMV EOM
            days_elapsed = (end_date - datetime(year_input, month_input, 1)).days + 1
            gmv_eom = calculate_gmv_eom(cumulative_gmv, days_elapsed, days_in_month)
            weeks_df.at[i, 'GMV EOM'] = gmv_eom

        # Simpan hasil di session state
        st.session_state['weeks_df'] = weeks_df

        # Menghitung nilai rata-rata
        avg_gmv = weeks_df['GMV Final Status'].mean()
        sum_gmv_eom = weeks_df[weeks_df['Minggu ke-'] == 4]['GMV EOM'].max()
        avg_orders_qty = weeks_df['Orders Qty'].mean()

        # Simpan hasil perhitungan ke session state
        st.session_state['avg_gmv'] = avg_gmv
        st.session_state['sum_gmv_eom'] = sum_gmv_eom
        st.session_state['avg_orders_qty'] = avg_orders_qty

        # ==== PROCESS DATA FOR PREVIOUS MONTH ====
        prev_weeks_df, prev_days_in_month = generate_weeks(previous_month, previous_year)
        cumulative_gmv_prev = Decimal(0)

        for i, row in stqdm(prev_weeks_df.iterrows(), total=prev_weeks_df.shape[0], desc="Processing Previous Month Weeks"):
            st.write(f"Processing week {i + 1} of previous month")
            start_date = datetime.strptime(row['Tanggal Senin (Awal Minggu)'], '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(row['Tanggal Minggu (Akhir Minggu)'], '%Y-%m-%d %H:%M:%S')

            start_timestamp = int(start_date.timestamp())
            end_timestamp = int(end_date.timestamp())

            # Perhitungan preStart dan preEnd
            c = end_timestamp - start_timestamp
            preStart_timestamp = start_timestamp - c
            preEnd_timestamp = end_timestamp - c

            query_prev = f"""
            SELECT
                COALESCE(SUM(CASE WHEN so.status IN (500, 702, 703) THEN so.gmv_shipment END), 0) AS gmv_final_status,
                COUNT(CASE WHEN so.status >= 300 AND so.status < 500 THEN 1 END) AS order_qty,
                AVG(so.transaction_value) AS aov
            FROM shipment_orders so
            WHERE so.created_at >= {start_timestamp} AND so.created_at <= {end_timestamp};
            """

            cur = connection.cursor()
            cur.execute(query_prev)
            result_prev = cur.fetchone()

            # Update DataFrame with query results
            prev_weeks_df.at[i, 'GMV Final Status'] = float(result_prev[0])  # Convert Decimal to float
            prev_weeks_df.at[i, 'Orders Qty'] = result_prev[1]
            prev_weeks_df.at[i, 'AOV'] = result_prev[2]

            # Calculate cumulative GMV up to the current week
            cumulative_gmv_prev += result_prev[0]

            # Calculate GMV EOM for previous month
            days_elapsed = (end_date - datetime(previous_year, previous_month, 1)).days + 1
            gmv_eom_prev = calculate_gmv_eom(cumulative_gmv_prev, days_elapsed, prev_days_in_month)
            prev_weeks_df.at[i, 'GMV EOM'] = gmv_eom_prev

        # Simpan hasil di session state untuk bulan sebelumnya
        st.session_state['prev_weeks_df'] = prev_weeks_df

        # Hitung rata-rata untuk bulan sebelumnya
        avg_gmv_prev = prev_weeks_df['GMV Final Status'].mean()
        sum_gmv_eom_prev = prev_weeks_df[prev_weeks_df['Minggu ke-'] == 4]['GMV EOM'].max()
        avg_orders_qty_prev = prev_weeks_df['Orders Qty'].mean()

        # Simpan hasil perhitungan ke session state untuk bulan sebelumnya
        st.session_state['avg_gmv_prev'] = avg_gmv_prev
        st.session_state['sum_gmv_eom_prev'] = sum_gmv_eom_prev
        st.session_state['avg_orders_qty_prev'] = avg_orders_qty_prev

        # Hitung delta untuk setiap metrics
        delta_gmv = (st.session_state['avg_gmv'] - avg_gmv_prev) / avg_gmv_prev * 100
        delta_gmv_eom = (st.session_state['sum_gmv_eom'] - sum_gmv_eom_prev) / sum_gmv_eom_prev * 100
        delta_orders_qty = (st.session_state['avg_orders_qty'] - avg_orders_qty_prev) / avg_orders_qty_prev * 100

        # Plot GMV Final Status chart
        # Dataframe 1
        first_columns = ['Tanggal Senin (Awal Minggu)', 'Tanggal Minggu (Akhir Minggu)', 'Minggu ke-', 'Bulan',
                         'GMV Final Status', 'GMV EOM', 'Orders Qty', 'R Transacting User', 'N Transacting User']
        df1 = st.session_state['weeks_df'][first_columns]

        # Dataframe 2
        second_columns = ['Tanggal Senin (Awal Minggu)', 'Tanggal Minggu (Akhir Minggu)', 'Minggu ke-', 'Bulan',
                          'AU (Aktive User)', 'TU (Trx User)', 'AOV', 'COD RTS%']
        df2 = st.session_state['weeks_df'][second_columns]
        st.dataframe(df1, use_container_width=True)
        st.dataframe(df2, use_container_width=True)

        # Button download for Excel
        if 'weeks_df' in st.session_state:
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                st.session_state['weeks_df'].to_excel(writer, index=False)
            processed_data = output.getvalue()

            file_name = f"weekly_report_{year_input}_{month_input:02d}.xlsx"

            st.download_button(label='Download as Excel',
                               data=processed_data,
                               file_name=file_name,
                               mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        # Show metrics
        # Display metrics with delta
        st.markdown('<hr>', unsafe_allow_html=True)

        left_column_stat, middle_column_stat, right_column_stat = st.columns(3)
        with left_column_stat:
            st.metric(label="Rata-rata GMV Final Status", value=f"{np.round(st.session_state['avg_gmv'], 2):,}",
                      delta=f"{np.round(delta_gmv, 2)}%")
        with middle_column_stat:
            st.metric(label="Total GMV End of Month", value=f"{np.round(st.session_state['sum_gmv_eom'], 2):,}",
                      delta=f"{np.round(delta_gmv_eom, 2)}%")
        with right_column_stat:
            st.metric(label="Rata-rata Orders QTY", value=f"{np.round(st.session_state['avg_orders_qty'], 2):,}",
                      delta=f"{np.round(delta_orders_qty, 2)}%")

        st.markdown('<hr>', unsafe_allow_html=True)

        st.markdown('<hr>', unsafe_allow_html=True)
        # ---

        fig_gmv = px.line(df1, x='Tanggal Senin (Awal Minggu)', y='GMV Final Status',
                          title='GMV Final Status per Minggu', labels={
                'Tanggal Senin (Awal Minggu)': 'Tanggal Minggu Awal',
                'GMV Final Status': 'GMV Final Status'
            })
        fig_gmv.update_traces(textposition='top center', mode='lines+markers+text',
                              text=df1['GMV Final Status'].apply(lambda x: f"{x:,.0f}"))
        fig_gmv.update_layout(yaxis_tickformat=',', showlegend=False)

        # Save chart to session
        st.session_state['fig_gmv'] = fig_gmv

        # Plot Orders Qty chart
        fig_orders = px.line(df1, x='Tanggal Senin (Awal Minggu)', y='Orders Qty',
                             title='Orders Qty per Minggu', labels={
                'Tanggal Senin (Awal Minggu)': 'Tanggal Minggu Awal',
                'Orders Qty': 'Jumlah Orders'
            })
        fig_orders.update_traces(textposition='top center', mode='lines+markers+text',
                                 text=df1['Orders Qty'].apply(lambda x: f"{x:,.0f}"))
        fig_orders.update_layout(yaxis_tickformat=',', showlegend=False)

        # Save chart to session
        st.session_state['fig_orders'] = fig_orders

        # Display charts
        st.plotly_chart(st.session_state['fig_gmv'])
        st.plotly_chart(st.session_state['fig_orders'])

        # ---

        # Plot Pie Chart for R Transacting User vs N Transacting User
        r_transacting_total = st.session_state['weeks_df']['R Transacting User'].sum()
        n_transacting_total = st.session_state['weeks_df']['N Transacting User'].sum()

        fig_pie_transacting = px.pie(values=[r_transacting_total, n_transacting_total],
                                     names=['R Transacting User', 'N Transacting User'],
                                     title='Perbandingan R Transacting User dan N Transacting User',
                                     labels={'value': 'Jumlah User', 'names': 'Kategori'})

        fig_pie_transacting.update_traces(textinfo='percent+label')
        fig_pie_transacting.update_layout(legend_title_text='Jenis User')

        # Save pie chart to session
        st.session_state['fig_pie_transacting'] = fig_pie_transacting

        # Plot Pie Chart for Active User vs Trx User
        active_user_total = st.session_state['weeks_df']['AU (Aktive User)'].sum()
        trx_user_total = st.session_state['weeks_df']['TU (Trx User)'].sum()

        fig_pie_active_trx = px.pie(values=[active_user_total, trx_user_total],
                                    names=['Aktive User', 'Transacting User'],
                                    title='Perbandingan Aktive User dan Transacting User',
                                    labels={'value': 'Jumlah User', 'names': 'Kategori'})

        fig_pie_active_trx.update_traces(textinfo='percent+label')
        fig_pie_active_trx.update_layout(legend_title_text='Jenis User')

        # Save pie chart to session
        st.session_state['fig_pie_active_trx'] = fig_pie_active_trx

        # Display Pie Charts in 2 columns
        left_col, right_col = st.columns(2)
        with left_col:
            st.plotly_chart(st.session_state['fig_pie_transacting'])
        with right_col:
            st.plotly_chart(st.session_state['fig_pie_active_trx'])

        # ---
        df2_bar = df2.copy()

        # Ubah sumbu X menjadi kategori (string) hanya untuk bar chart
        df2_bar['Tanggal Senin (Awal Minggu)'] = df2_bar['Tanggal Senin (Awal Minggu)'].astype(str)

        # Plot Bar Chart for AOV per Minggu
        fig_bar_aov = px.bar(df2_bar, x='Tanggal Senin (Awal Minggu)', y='AOV',
                             title='Rata-rata Nilai Pesanan (AOV) per Minggu',
                             labels={'Tanggal Senin (Awal Minggu)': 'Tanggal Minggu Awal',
                                     'AOV': 'Rata-rata Nilai Pesanan (AOV)'})

        fig_bar_aov.update_traces(texttemplate='%{y:,.0f}', textposition='outside')
        fig_bar_aov.update_layout(yaxis_tickformat=',', showlegend=False)

        # Save bar chart to session
        st.session_state['fig_bar_aov'] = fig_bar_aov

        # Plot Line Chart for COD RTS per Minggu
        fig_line_cod_rts = px.line(df2, x='Tanggal Senin (Awal Minggu)', y='COD RTS%',
                                   title='COD RTS per Minggu',
                                   labels={'Tanggal Senin (Awal Minggu)': 'Tanggal Minggu Awal',
                                           'COD RTS%': 'Persentase COD RTS'})

        fig_line_cod_rts.update_traces(textposition='top center', mode='lines+markers+text',
                                       text=df2['COD RTS%'].apply(lambda x: f"{x:.2%}"))
        # Update sumbu Y menjadi persentase
        fig_line_cod_rts.update_layout(yaxis_tickformat='.2%', showlegend=False)

        # Save line chart to session
        st.session_state['fig_line_cod_rts'] = fig_line_cod_rts

        # Display Bar and Line Charts in 2 columns
        left_col, right_col = st.columns(2)
        with left_col:
            st.plotly_chart(st.session_state['fig_bar_aov'])
        with right_col:
            st.plotly_chart(st.session_state['fig_line_cod_rts'])
