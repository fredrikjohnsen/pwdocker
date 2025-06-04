import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import sys

# Add parent directory to path to import storage module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import Storage

# Page config
st.set_page_config(
    page_title="PWConvert Dashboard",
    page_icon="🔄",
    layout="wide"
)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

st.title("🔄 PWConvert Dashboard")
st.markdown("Monitor and analyze your file conversion progress")

# Database connection
@st.cache_data(ttl=30)  # Cache for 30 seconds
def get_conversion_data():
    """Fetch conversion data from database"""
    try:
        with Storage(os.getenv('DB_HOST', 'mysql')) as store:
            # Get all files data
            query = """
            SELECT 
                id, path, size, mime, format, version, status, puid, 
                created_at, updated_at, status_ts, error_message,
                target_path, kept, original, finished, subpath
            FROM file 
            ORDER BY created_at DESC
            """
            cursor = store.connection.cursor(dictionary=True)
            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()
            
            if data:
                df = pd.DataFrame(data)
                # Convert datetime columns
                for col in ['created_at', 'updated_at', 'status_ts']:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col])
                return df
            else:
                return pd.DataFrame()
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return pd.DataFrame()

def clear_file_table():
    """Clear all records from the file table"""
    try:
        with Storage(os.getenv('DB_HOST', 'mysql')) as store:
            cursor = store.connection.cursor()
            cursor.execute("DELETE FROM file")
            rows_deleted = cursor.rowcount
            cursor.close()
            return rows_deleted
    except Exception as e:
        st.error(f"Error clearing table: {e}")
        return 0

def get_table_stats():
    """Get basic table statistics"""
    try:
        with Storage(os.getenv('DB_HOST', 'mysql')) as store:
            cursor = store.connection.cursor(dictionary=True)
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN status = 'converted' THEN 1 END) as converted_files,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_files,
                    COUNT(CASE WHEN status = 'new' THEN 1 END) as new_files,
                    SUM(size) as total_size
                FROM file
            """)
            stats = cursor.fetchone()
            cursor.close()
            return stats
    except Exception as e:
        st.error(f"Error getting table stats: {e}")
        return None

# Sidebar - Always show this section
st.sidebar.header("🔄 PWConvert Controls")

# Add clear table section in sidebar - ALWAYS VISIBLE
st.sidebar.header("⚠️ Danger Zone")
st.sidebar.markdown("**Clear Database Table**")

# Get current table stats
stats = get_table_stats()
if stats and stats['total_files'] > 0:
    st.sidebar.markdown(f"""
    **Current table contains:**
    - {stats['total_files']} total files
    - {stats['converted_files']} converted
    - {stats['failed_files']} failed  
    - {stats['new_files']} new
    - {stats['total_size'] / (1024**3):.2f} GB total size
    """)
else:
    st.sidebar.markdown("**Table is currently empty**")

# Confirmation checkbox
confirm_clear = st.sidebar.checkbox("I understand this will delete ALL data")

# Clear button - ALWAYS VISIBLE
if st.sidebar.button("🗑️ Clear All Records", type="secondary", disabled=not confirm_clear):
    if confirm_clear:
        with st.spinner("Clearing database table..."):
            rows_deleted = clear_file_table()
            if rows_deleted > 0:
                st.sidebar.success(f"Successfully deleted {rows_deleted} records!")
                st.cache_data.clear()  # Clear cache to refresh data
                st.rerun()  # Changed from st.experimental_rerun()
            else:
                st.sidebar.warning("No records were deleted or table was already empty.")
    else:
        st.sidebar.error("Please confirm by checking the checkbox above.")

# Sidebar filters section
st.sidebar.header("📊 Filters")

# Get data
df = get_conversion_data()

# Check if data is empty AFTER showing the clear controls
if df.empty:
    st.warning("No data found in the database. Run some conversions first or clear the table to start fresh!")
    st.info("💡 Use the 'Clear All Records' button in the sidebar if you want to reset the database.")
    st.stop()

# Status filter
status_options = ['All'] + list(df['status'].unique())
selected_status = st.sidebar.selectbox("Status", status_options)

# Date range filter
if 'created_at' in df.columns:
    min_date = df['created_at'].min().date()
    max_date = df['created_at'].max().date()
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        df = df[
            (df['created_at'].dt.date >= start_date) & 
            (df['created_at'].dt.date <= end_date)
        ]

# Apply status filter
if selected_status != 'All':
    df = df[df['status'] == selected_status]

# Main dashboard
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Files", len(df))

with col2:
    if 'status' in df.columns:
        converted_count = len(df[df['status'] == 'converted'])
        st.metric("Converted", converted_count)

with col3:
    if 'status' in df.columns:
        failed_count = len(df[df['status'] == 'failed'])
        st.metric("Failed", failed_count, delta=f"-{failed_count}")

with col4:
    if 'size' in df.columns:
        total_size = df['size'].sum() / (1024**3)  # Convert to GB
        st.metric("Total Size", f"{total_size:.2f} GB")

# Charts
st.header("📊 Analytics")

col1, col2 = st.columns(2)

with col1:
    # Status distribution
    if 'status' in df.columns:
        status_counts = df['status'].value_counts()
        fig_status = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Status Distribution"
        )
        st.plotly_chart(fig_status, use_container_width=True)

with col2:
    # File types distribution
    if 'mime' in df.columns:
        mime_counts = df['mime'].value_counts().head(10)
        fig_mime = px.bar(
            x=mime_counts.values,
            y=mime_counts.index,
            orientation='h',
            title="Top 10 File Types"
        )
        fig_mime.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_mime, use_container_width=True)

# Timeline chart
if 'created_at' in df.columns:
    st.header("📈 Conversion Timeline")
    
    # Group by date
    df['date'] = df['created_at'].dt.date
    timeline_data = df.groupby(['date', 'status']).size().reset_index(name='count')
    
    fig_timeline = px.line(
        timeline_data,
        x='date',
        y='count',
        color='status',
        title="Conversions Over Time"
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

# Error Analysis
if 'error_message' in df.columns:
    error_df = df[df['error_message'].notna()]
    if not error_df.empty:
        st.header("❌ Error Analysis")
        
        # Most common errors
        error_counts = error_df['error_message'].value_counts().head(10)
        fig_errors = px.bar(
            x=error_counts.values,
            y=error_counts.index,
            orientation='h',
            title="Most Common Errors"
        )
        fig_errors.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_errors, use_container_width=True)

# Detailed data table
st.header("📋 File Details")

# Search functionality
search_term = st.text_input("Search files by path or name:")
if search_term:
    df = df[df['path'].str.contains(search_term, case=False, na=False)]

# Display options
show_columns = st.multiselect(
    "Select columns to display:",
    options=df.columns.tolist(),
    default=['path', 'status', 'mime', 'size', 'created_at']
)

if show_columns:
    # Format the dataframe for display
    display_df = df[show_columns].copy()
    
    # Format file sizes
    if 'size' in display_df.columns:
        display_df['size'] = display_df['size'].apply(
            lambda x: f"{x/1024/1024:.1f} MB" if pd.notna(x) else "N/A"
        )
    
    # Format dates
    for col in ['created_at', 'updated_at', 'status_ts']:
        if col in display_df.columns:
            display_df[col] = display_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    st.dataframe(display_df, use_container_width=True)

# Export functionality
st.header("📥 Export Data")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("Download CSV"):
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download CSV file",
            data=csv,
            file_name=f"pwconvert_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

with col2:
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()  # Changed from st.experimental_rerun()

with col3:
    # Additional clear button in main area (always visible when data exists)
    if st.button("🗑️ Clear Database", type="secondary"):
        st.error("Use the 'Clear All Records' button in the sidebar for safety.")