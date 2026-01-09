import streamlit as st
import pandas as pd
import zipfile
import io
from pathlib import Path
import base64

# Page configuration
st.set_page_config(
    page_title="Sales vs Return Data Analyzer",
    page_icon="📊",
    layout="wide"
)

# Set configuration at the top
st.set_option("server.fileWatcherType", "none")

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        color: #1e40af;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #4b5563;
        text-align: center;
        margin-bottom: 3rem;
    }
    .metric-card {
        background-color: #f0f9ff;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #3b82f6;
    }
    </style>
""", unsafe_allow_html=True)

# Helper Functions
def read_zip_files(zip_files):
    """Read and combine data from multiple zip files"""
    all_data = []
    
    for zip_file in zip_files:
        with zipfile.ZipFile(io.BytesIO(zip_file.read()), 'r') as z:
            for file_name in z.namelist():
                if file_name.endswith(('.xlsx', '.xls', '.csv')):
                    with z.open(file_name) as f:
                        if file_name.endswith('.csv'):
                            df = pd.read_csv(f, low_memory=False)
                        else:
                            df = pd.read_excel(f, engine='openpyxl')
                        
                        df["Source_Zip"] = zip_file.name
                        df["Source_File"] = file_name
                        all_data.append(df)
    
    if all_data:
        return pd.concat(all_data, ignore_index=True, copy=False)
    return pd.DataFrame()

def remove_byte_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that contain raw bytes (Arrow cannot serialize them)"""
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().head(1)
            if not sample.empty and isinstance(sample.iloc[0], (bytes, bytearray)):
                df = df.drop(columns=[col])
    return df


def process_combined_data(combined_df):
    """Filter and clean combined data"""
    # Filter for Shipment transactions only (vectorized operation)
    mask = combined_df["Transaction Type"].astype(str).str.strip().str.lower() == "shipment"
    combined_df = combined_df[mask].copy()
    
    # Remove zero invoice amounts (vectorized operation)
    combined_df["Invoice Amount"] = pd.to_numeric(combined_df["Invoice Amount"], errors="coerce")
    combined_df = combined_df[combined_df["Invoice Amount"] != 0]
    
    return combined_df

def merge_product_master(df, pm_df):
    """Merge combined data with product master"""
    pm_cols = ["ASIN", "Brand", "Brand Manager", "Vendor SKU Codes", "CP"]
    pm_clean = pm_df[pm_cols].drop_duplicates(subset=["ASIN"]).copy()
    
    merged_df = df.merge(
        pm_clean,
        left_on="Asin",
        right_on="ASIN",
        how="left",
        copy=False
    )
    
    merged_df["CP"] = pd.to_numeric(merged_df["CP"], errors="coerce")
    merged_df["Quantity"] = pd.to_numeric(merged_df["Quantity"], errors="coerce")

    # ✅ NEW COLUMN
    merged_df["CP As Per Qty"] = merged_df["CP"] * merged_df["Quantity"]
    
    return merged_df

def create_brand_pivot(df):
    """Create brand-level pivot table"""
    return df.pivot_table(
        index="Brand",
        values="Quantity",
        aggfunc="sum"
    ).reset_index().sort_values("Quantity", ascending=False)

def create_asin_pivot(df):
    """Create ASIN-level pivot table"""
    return df.pivot_table(
        index="Asin",
        values="Quantity",
        aggfunc="sum"
    ).reset_index().sort_values("Quantity", ascending=False)

def create_asin_final_summary(asin_qty_pivot, fba_return_asin, seller_flex_asin):
    """Create final ASIN summary with returns"""
    # Rename columns for FBA and Seller Flex
    if fba_return_asin is not None:
        fba_return_asin = fba_return_asin.rename(columns={"quantity": "FBA Return", "asin": "Asin"})
    
    if seller_flex_asin is not None:
        seller_flex_asin = seller_flex_asin.rename(columns={"Units": "Seller Flex", "ASIN": "Asin"})
    
    # Start with quantity pivot
    result = asin_qty_pivot.copy()
    
    # Merge FBA returns
    if fba_return_asin is not None:
        result = result.merge(
            fba_return_asin[["Asin", "FBA Return"]],
            on="Asin",
            how="left"
        )
    else:
        result["FBA Return"] = 0
    
    # Merge Seller Flex returns
    if seller_flex_asin is not None:
        result = result.merge(
            seller_flex_asin[["Asin", "Seller Flex"]],
            on="Asin",
            how="left"
        )
    else:
        result["Seller Flex"] = 0
    
    # Calculate total returns
    result["Total Return"] = (
        result["FBA Return"].fillna(0) +
        result["Seller Flex"].fillna(0)
    )
    
    # Calculate return percentage
    result["Return In %"] = (
        (result["Total Return"] / result["Quantity"]) * 100
    ).round(2)
    
    # Sort by Quantity descending
    result = result.sort_values("Quantity", ascending=False)
    
    return result

def process_seller_flex(df, pm_df):
    """Process Seller Flex data"""
    # Clean columns
    cols_to_remove = [
        "External ID1", "External ID2", "External ID3",
        "Forward Leg Tracking ID", "Reverse Leg Tracking ID", "RMA ID",
        "Return Status", "Carrier", "Pick -up date", "Last Updated On",
        "Returned with OTP", "Days In-transit", "Days Since Return Complete",
        "Return Reason"
    ]
    df = df.drop(columns=cols_to_remove, errors="ignore")
    
    # Create combine column (vectorized)
    df["Combine"] = df["Customer Order ID"].astype(str).str.strip() + df["ASIN"].astype(str).str.strip()
    
    # Remove duplicates
    df = df.drop_duplicates(keep='first')
    
    # Merge with product master
    pm_cols = ["ASIN", "Brand", "Brand Manager", "Vendor SKU Codes", "CP"]
    pm_clean = pm_df[pm_cols].drop_duplicates(subset=["ASIN"]).copy()
    
    df = df.merge(pm_clean, left_on="ASIN", right_on="ASIN", how="left", copy=False)
    
    return df

def process_fba_return(df, pm_df):
    """Process FBA Return data"""
    pm_cols = ["ASIN", "Brand", "Brand Manager", "Vendor SKU Codes", "CP"]
    pm_clean = pm_df[pm_cols].drop_duplicates(subset=["ASIN"]).copy()
    
    df = df.merge(pm_clean, left_on="asin", right_on="ASIN", how="left", copy=False)
    
    return df

def create_final_summary(brand_qty_pivot, brand_fba_pivot, brand_seller_pivot):
    """Create final brand summary with returns"""
    # Rename columns
    brand_fba_pivot = brand_fba_pivot.rename(columns={"quantity": "FBA Return"})
    brand_seller_pivot = brand_seller_pivot.rename(columns={"Units": "Seller Flex"})
    
    # Merge all data
    result = brand_qty_pivot.merge(
        brand_fba_pivot[["Brand", "FBA Return"]],
        on="Brand",
        how="left"
    )
    
    result = result.merge(
        brand_seller_pivot[["Brand", "Seller Flex"]],
        on="Brand",
        how="left"
    )
    
    # Calculate total returns
    result["Total Return"] = (
        result["FBA Return"].fillna(0) +
        result["Seller Flex"].fillna(0)
    )
    
    # Calculate return percentage
    result["Return In %"] = (
        (result["Total Return"] / result["Quantity"]) * 100
    ).round(2)
    
    return result

@st.cache_data
def convert_df_to_excel(df):
    """Convert dataframe to excel bytes with caching"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def create_download_button(df, filename, button_text="📥 Download Excel"):
    """Create a download button for dataframe"""
    excel_data = convert_df_to_excel(df)

    st.download_button(
        label=button_text,
        data=excel_data,
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )


# Main App
st.markdown('<h1 class="main-header">📊 Sales vs Return Data Analyzer</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Upload your data files to generate comprehensive reports</p>', unsafe_allow_html=True)

# Initialize session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
    st.session_state.results = {}

# File Upload Section
col1, col2 = st.columns(2)

with col1:
    st.subheader("📦 B2B Reports (ZIP)")
    b2b_files = st.file_uploader(
        "Upload B2B ZIP files",
        type=['zip'],
        accept_multiple_files=True,
        key='b2b'
    )
    
    st.subheader("📦 B2C Reports (ZIP)")
    b2c_files = st.file_uploader(
        "Upload B2C ZIP files",
        type=['zip'],
        accept_multiple_files=True,
        key='b2c'
    )
    
    st.subheader("📄 Seller Flex Report (CSV)")
    seller_flex_file = st.file_uploader(
        "Upload Seller Flex CSV",
        type=['csv'],
        key='seller_flex'
    )

with col2:
    st.subheader("📄 FBA Return Report (CSV)")
    fba_return_file = st.file_uploader(
        "Upload FBA Return CSV",
        type=['csv'],
        key='fba_return'
    )
    
    st.subheader("📋 Purchase Master (XLSX)")
    product_master_file = st.file_uploader(
        "Upload Product Master Excel",
        type=['xlsx', 'xls'],
        key='product_master'
    )
    
# Process Button
st.markdown("---")
process_button = st.button("🚀 Process Data", use_container_width=True, type="primary")

if process_button:
    if not (b2b_files or b2c_files):
        st.error("Please upload at least one B2B or B2C report file.")
    else:
        with st.spinner("Processing your data..."):
            try:
                # Combine zip files
                all_zip_files = (b2b_files or []) + (b2c_files or [])
                combined_df = read_zip_files(all_zip_files)
                
                if combined_df.empty:
                    st.error("No data found in the uploaded files.")
                else:
                    # Process combined data
                    combined_df = process_combined_data(combined_df)
                    combined_df = remove_byte_columns(combined_df)

                    # Load product master
                    if product_master_file:
                        pm_df = pd.read_excel(product_master_file)
                        combined_df = merge_product_master(combined_df, pm_df)
                        
                    # Create pivots
                    brand_qty_pivot = create_brand_pivot(combined_df)
                    asin_qty_pivot = create_asin_pivot(combined_df)
                    
                    # Process Seller Flex
                    seller_flex_df = None
                    seller_flex_brand = None
                    seller_flex_asin = None
                    
                    if seller_flex_file and product_master_file:
                        seller_flex_df = pd.read_csv(seller_flex_file)
                        seller_flex_df = process_seller_flex(seller_flex_df, pm_df)
                        seller_flex_df = remove_byte_columns(seller_flex_df)
                        
                        seller_flex_brand = seller_flex_df.pivot_table(
                            index="Brand",
                            values="Units",
                            aggfunc="sum"
                        ).reset_index().sort_values("Units", ascending=False)
                        
                        seller_flex_asin = seller_flex_df.pivot_table(
                            index="ASIN",
                            values="Units",
                            aggfunc="sum"
                        ).reset_index().sort_values("Units", ascending=False)
                    
                    # Process FBA Return
                    fba_return_df = None
                    fba_return_brand = None
                    fba_return_asin = None
                    
                    if fba_return_file and product_master_file:
                        fba_return_df = pd.read_csv(fba_return_file)
                        fba_return_df = process_fba_return(fba_return_df, pm_df)
                        fba_return_df = remove_byte_columns(fba_return_df)

                        fba_return_brand = fba_return_df.pivot_table(
                            index="Brand",
                            values="quantity",
                            aggfunc="sum"
                        ).reset_index().sort_values("quantity", ascending=False)
                        
                        fba_return_asin = fba_return_df.pivot_table(
                            index="asin",
                            values="quantity",
                            aggfunc="sum"
                        ).reset_index().sort_values("quantity", ascending=False)
                    
                    # Create final summaries
                    if fba_return_brand is not None and seller_flex_brand is not None:
                        brand_final = create_final_summary(
                            brand_qty_pivot,
                            fba_return_brand,
                            seller_flex_brand
                        )
                    else:
                        brand_final = brand_qty_pivot
                    
                    # Create ASIN final summary with returns
                    if fba_return_asin is not None or seller_flex_asin is not None:
                        asin_final = create_asin_final_summary(
                            asin_qty_pivot,
                            fba_return_asin,
                            seller_flex_asin
                        )
                    else:
                        asin_final = asin_qty_pivot
                    
                    # Store results
                    st.session_state.results = {
                        'combined_df': combined_df,
                        'brand_qty_pivot': brand_qty_pivot,
                        'asin_qty_pivot': asin_qty_pivot,
                        'asin_final': asin_final,
                        'seller_flex_df': seller_flex_df,
                        'seller_flex_brand': seller_flex_brand,
                        'seller_flex_asin': seller_flex_asin,
                        'fba_return_df': fba_return_df,
                        'fba_return_brand': fba_return_brand,
                        'fba_return_asin': fba_return_asin,
                        'brand_final': brand_final
                    }
                    st.session_state.processed = True
                    st.success("✅ Data processed successfully!")
                    # st.rerun()
                    
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

# Display Results
if st.session_state.processed:
    st.markdown("---")
    st.markdown("## 📊 Analysis Results")
    
    results = st.session_state.results
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", f"{len(results['combined_df']):,}")
    with col2:
        st.metric("Total Brands", f"{len(results['brand_qty_pivot']):,}")
    with col3:
        st.metric("Total ASINs", f"{len(results['asin_qty_pivot']):,}")
    with col4:
        if results['seller_flex_df'] is not None:
            st.metric("Seller Flex Returns", f"{len(results['seller_flex_df']):,}")
    
    # Tabs for different reports
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Combined Data",
        "🏷️ Brand Analysis",
        "🔖 ASIN Analysis",
        "📦 Seller Flex",
        "↩️ FBA Returns"
    ])
    
    with tab1:
        st.subheader("Combined Transaction Data")
        st.dataframe(results['combined_df'].head(100), use_container_width=True)
        create_download_button(results['combined_df'], "combined_data_report.xlsx")
    
    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Brand Quantity Pivot")
            st.dataframe(results['brand_qty_pivot'], use_container_width=True)
            create_download_button(results['brand_qty_pivot'], "brand_quantity_pivot.xlsx")
        
        with col2:
            st.subheader("Brand Final Summary (with Returns)")
            st.dataframe(results['brand_final'], use_container_width=True)
            create_download_button(results['brand_final'], "brand_final_summary.xlsx")
    
    with tab3:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("ASIN Quantity Pivot")
            st.dataframe(results['asin_qty_pivot'], use_container_width=True)
            create_download_button(results['asin_qty_pivot'], "asin_quantity_pivot.xlsx")
        
        with col2:
            if 'asin_final' in results and results['asin_final'] is not None:
                st.subheader("ASIN Final Summary (with Returns)")
                st.dataframe(results['asin_final'], use_container_width=True)
                create_download_button(results['asin_final'], "asin_final_summary.xlsx")
    
    with tab4:
        if results['seller_flex_df'] is not None:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Seller Flex - Brand Pivot")
                st.dataframe(results['seller_flex_brand'],use_container_width=True)
                create_download_button(results['seller_flex_brand'], "seller_flex_brand.xlsx")
            
            with col2:
                st.subheader("Seller Flex - ASIN Pivot")
                st.dataframe(results['seller_flex_asin'], use_container_width=True)
                create_download_button(results['seller_flex_asin'], "seller_flex_asin.xlsx")
        else:
            st.info("No Seller Flex data uploaded")
    
    with tab5:
        if results['fba_return_df'] is not None:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("FBA Return - Brand Pivot")
                st.dataframe(results['fba_return_brand'], use_container_width=True)
                create_download_button(results['fba_return_brand'], "fba_return_brand.xlsx")
            
            with col2:
                st.subheader("FBA Return - ASIN Pivot")
                st.dataframe(results['fba_return_asin'], use_container_width=True)
                create_download_button(results['fba_return_asin'], "fba_return_asin.xlsx")
        else:
            st.info("No FBA Return data uploaded")
    
    # Download All Button
    st.markdown("---")
    st.subheader("📥 Download All Reports")
    
    if st.button("Download All Reports as ZIP", use_container_width=True):
        # Create ZIP file with all reports
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for name, df in results.items():
                if df is not None and isinstance(df, pd.DataFrame):
                    # Use the cached converter for each file in the ZIP too
                    excel_bytes = convert_df_to_excel(df)
                    zip_file.writestr(f"{name}.xlsx", excel_bytes)
        
        st.download_button(
            label="📦 Download ZIP",
            data=zip_buffer.getvalue(),
            file_name="all_reports.zip",
            mime="application/zip",
            use_container_width=True
        )

# Footer
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #6b7280; padding: 2rem;'>
        <p>Upload your B2B/B2C reports, Seller Flex data, FBA returns, and Product Master to generate comprehensive analytics</p>
        <p style='font-size: 0.875rem;'>Supported formats: ZIP (B2B/B2C), CSV (Seller Flex, FBA Return), XLSX (Product Master)</p>
    </div>
""", unsafe_allow_html=True)
