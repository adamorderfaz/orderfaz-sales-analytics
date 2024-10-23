# ----- Import Library -----
import pandas as pd
import numpy as np
import streamlit as st
from PIL import Image

# ----- Load images as icon -----
icon_image = Image.open("orderfaz.jpeg")

# ----- Set Page Sidebar -----
p1 = st.Page("pages/dashboard1.py", title="GMV Weekly", icon=":material/date_range:")
p2 = st.Page("pages/dashboard2.py", title="GMV Monthly", icon=":material/calendar_month:")
p3 = st.Page("pages/dashboard3.py", title="Top Revenue", icon=":material/hourglass_top:")

# ----- Install Multi-page Navigation -----
pg = st.navigation({
    "Home": [st.Page("home.py", title="Home", icon=":material/home:")],
    "Menu": [p1, p2, p3]
})

# ----- Set Page Config -----
st.set_page_config(page_title="OF | Sales", page_icon=icon_image, layout="wide")

# ----- Run Streamlit Page Navigation -----
pg.run()

# Share Info across all pages (optional)
# ----- Set Logo -----
st.logo('orderfaz.jpeg')
st.sidebar.header(" ☎️ Adam Maurizio")


