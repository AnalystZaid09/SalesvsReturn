import streamlit as st
import pandas as pd
import zipfile
import io
import gc
from pathlib import Path
import base64
import traceback

# Page configuration
st.set_page_config(
    page_title="Sales vs Return Data Analyzer",
    page_icon="📊",
    layout="wide"
)
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
    """Read and combine data from multiple zip files efficiently"""
    all_data = []
    
    # Define common dtypes to save memory
    dtypes = {
        "Transaction Type": "category",
        "Quantity": "float32",
        "Invoice Amount": "float32"
    }
    
    for zip_file in zip_files:
        try:
            # zip_file is already a seekable file-like object in Streamlit
            with zipfile.ZipFile(zip_file, 'r') as z:
                for file_name in z.namelist():
                    if file_name.endswith(('.xlsx', '.xls', '.csv')):
                        with z.open(file_name) as f:
                            if file_name.endswith('.csv'):
                                # Read CSV with low_memory=True and dtypes
                                df = pd.read_csv(f, low_memory=True, dtype=dtypes)
                            else:
                                df = pd.read_excel(f, engine='openpyxl')
                            
                            df["Source_Zip"] = zip_file.name
                            df["Source_File"] = file_name
                            
                            # Normalize column names to Title Case and deduplicate
                            new_cols = []
                            seen = {}
                            for c in df.columns:
                                base = str(c).strip().title()
                                if base in seen:
                                    seen[base] += 1
                                    new_cols.append(f"{base}_{seen[base]}")
                                else:
                                    seen[base] = 0
                                    new_cols.append(base)
                            df.columns = new_cols
                            
                            all_data.append(df)
                            gc.collect()
        except Exception as e:
            st.warning(f"Could not read zip file {zip_file.name}: {str(e)}")
    
    if all_data:
        combined = pd.concat(all_data, ignore_index=True, copy=False)
        all_data.clear()
        gc.collect()
        return combined
    return pd.DataFrame()

def add_grand_total(df):
    """Add a Grand Total row to the dataframe for numeric columns"""
    if df is None or df.empty:
        return df
    
    # Identify numeric columns
    # We exclude columns that should not be summed even if numeric (like ASIN if it's all digits, though unlikely)
    exclude_cols = ['ASIN', 'Asin', 'asin', 'CP'] # CP (unit cost) shouldn't be summed
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    numeric_cols = [col for col in numeric_cols if col not in exclude_cols]
    
    if not numeric_cols:
        return df
        
    # Create total row
    total_row = {}
    for col in df.columns:
        if col in numeric_cols:
            if col == "Return In %":
                total_row[col] = 0 # Placeholder
            else:
                total_row[col] = df[col].sum()
        else:
            total_row[col] = "" # Empty for non-numeric
            
    # Set Grand Total label in the first column
    first_col = df.columns[0]
    total_row[first_col] = "Grand Total"
    
    # Recalculate Return In % for the total row if possible
    if "Return In %" in df.columns:
        qty_col = next((c for c in ["Quantity", "quantity", "Units"] if c in df.columns), None)
        ret_col = next((c for c in ["Total Return", "quantity", "Units", "FBA Return", "Seller Flex"] if c in df.columns and c != qty_col), None)
        
        # Specific check for final summary reports
        qty_val = total_row.get("Quantity") or total_row.get("quantity") or total_row.get("Units")
        ret_val = total_row.get("Total Return")
        
        if qty_val and ret_val and qty_val > 0:
            total_row["Return In %"] = round((ret_val / qty_val) * 100, 2)
        elif "Return In %" in numeric_cols:
             total_row["Return In %"] = 0 # Cannot sum percentages

    return pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

def ensure_arrow_compatibility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Faster version of Arrow compatibility check.
    Only touches object columns and handles NaNs.
    """
    if df is None or df.empty:
        return df
    
    # Process only object columns. Numeric are already Arrow-safe.
    obj_cols = df.select_dtypes(include=['object']).columns
    for col in obj_cols:
        # Cast to string - this solves mixed type issues and Arrow serialization
        df[col] = df[col].astype(str).replace(['nan', 'None', 'NaN', 'NAT', 'nat'], '')
    
    return df


def process_combined_data(combined_df):
    """Filter and clean combined data"""
    # Filter for Shipment transactions only (vectorized operation)
    mask = combined_df["Transaction Type"].astype(str).str.strip().str.lower() == "shipment"
    combined_df = combined_df[mask].copy()
    
    # Remove zero invoice amounts (vectorized operation)
    combined_df["Invoice Amount"] = pd.to_numeric(combined_df["Invoice Amount"], errors="coerce")
    combined_df = combined_df[combined_df["Invoice Amount"] != 0]
    
    # Remove Return Type if exists to keep report clean
    if "Return Type" in combined_df.columns:
        combined_df = combined_df.drop(columns=["Return Type"])
    
    return combined_df

def merge_product_master(df, pm_df):
    """Merge combined data with purchase master"""
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

def create_asin_final_summary(asin_qty_pivot, fba_return_asin, seller_flex_asin, pm_df=None, fba_disposition_pivot=None):
    """Create final ASIN summary with returns and purchase details from PM file"""
    # Rename columns for FBA and Seller Flex
    if fba_return_asin is not None:
        fba_return_asin = fba_return_asin.rename(columns={"quantity": "FBA Return", "asin": "Asin"})
    
    if seller_flex_asin is not None:
        seller_flex_asin = seller_flex_asin.rename(columns={"Units": "Seller Flex", "ASIN": "Asin"})
    
    # Start with quantity pivot
    result = asin_qty_pivot.copy()
    
    # Merge Brand, Product Name, and Vendor SKU Codes from PM file
    if pm_df is not None:
        pm_cols = ["ASIN", "Brand", "Product Name", "Vendor SKU Codes"]
        available_cols = [col for col in pm_cols if col in pm_df.columns]
        if available_cols:
            pm_clean = pm_df[available_cols].drop_duplicates(subset=["ASIN"]).copy()
            result = result.merge(
                pm_clean,
                left_on="Asin",
                right_on="ASIN",
                how="left"
            )
            # Drop duplicate ASIN column from PM
            if "ASIN" in result.columns:
                result = result.drop(columns=["ASIN"])
    
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
    
    # Merge FBA Disposition columns
    disposition_cols = []
    if fba_disposition_pivot is not None:
        # Rename asin column to match
        disp_df = fba_disposition_pivot.copy()
        if "asin" in disp_df.columns:
            disp_df = disp_df.rename(columns={"asin": "Asin"})
        
        # Get disposition columns (all columns except Asin and Total)
        disposition_cols = [col for col in disp_df.columns if col not in ["Asin", "Total"]]
        
        # Merge disposition data
        result = result.merge(
            disp_df,
            on="Asin",
            how="left"
        )
        
        # Fill NaN values with 0 for disposition columns
        for col in disposition_cols:
            if col in result.columns:
                result[col] = result[col].fillna(0).astype(int)
        
        # Rename Total from disposition pivot to Disposition Total
        if "Total" in result.columns:
            result = result.rename(columns={"Total": "Disposition Total"})
            result["Disposition Total"] = result["Disposition Total"].fillna(0).astype(int)
    
    # Reorder columns to put product info near the front, disposition cols at the end
    desired_order = ["Asin", "Brand", "Product Name", "Vendor SKU Codes", "Quantity", 
                     "FBA Return", "Seller Flex", "Total Return", "Return In %"]
    existing_cols = [col for col in desired_order if col in result.columns]
    # Add disposition columns at the end
    if disposition_cols:
        existing_cols = existing_cols + disposition_cols + ["Disposition Total"]
    other_cols = [col for col in result.columns if col not in existing_cols]
    result = result[existing_cols + other_cols]
    
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
    
    # Create combine column for duplicate detection (without Return Type)
    df["Combine"] = df["Customer Order ID"].astype(str).str.strip() + df["ASIN"].astype(str).str.strip()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=["Combine"], keep='first')
    
    # Merge with product master
    pm_cols = ["ASIN", "Brand", "Brand Manager", "Vendor SKU Codes", "CP"]
    pm_clean = pm_df[pm_cols].drop_duplicates(subset=["ASIN"]).copy()
    
    df = df.merge(pm_clean, left_on="ASIN", right_on="ASIN", how="left", copy=False)
    
    # Cleanup: Remove Return Type but keep temporary Combine column as requested
    cols_to_drop = ["Return Type"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")
    
    return df

def process_fba_return(df, pm_df):
    """Process FBA Return data"""
    pm_cols = ["ASIN", "Brand", "Brand Manager", "Vendor SKU Codes", "CP"]
    pm_clean = pm_df[pm_cols].drop_duplicates(subset=["ASIN"]).copy()
    
    df = df.merge(pm_clean, left_on="asin", right_on="ASIN", how="left", copy=False)
    
    # Remove Return Type if exists
    if "Return Type" in df.columns:
        df = df.drop(columns=["Return Type"])
    
    return df

def create_final_summary(brand_qty_pivot, brand_fba_pivot, brand_seller_pivot, fba_disposition_brand_pivot=None):
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

    # Merge FBA Disposition columns
    disposition_cols = []
    if fba_disposition_brand_pivot is not None:
        # Get disposition columns (all columns except Brand)
        disposition_cols = [col for col in fba_disposition_brand_pivot.columns if col != "Brand"]
        
        # Merge disposition data
        result = result.merge(
            fba_disposition_brand_pivot,
            on="Brand",
            how="left"
        )
        
        # Fill NaN values with 0 for disposition columns
        for col in disposition_cols:
            if col in result.columns:
                result[col] = result[col].fillna(0).astype(int)
        
        # Calculate/Overwritre Disposition Total for Brand
        # This sums the disposition columns we just merged
        valid_disp_cols = [col for col in disposition_cols if col in result.columns]
        if valid_disp_cols:
            result["Disposition Total"] = result[valid_disp_cols].sum(axis=1)

    return result

@st.cache_data(show_spinner=False)
def convert_df_to_excel(df):
    """Convert dataframe to excel bytes. Cached to prevent re-generation on every rerun."""
    output = io.BytesIO()
    # Try xlsxwriter for better performance, fallback to openpyxl
    try:
        # Use xlsxwriter with constant_memory to reduce RAM usage for large sheets
        with pd.ExcelWriter(output, engine='xlsxwriter', engine_kwargs={'options': {'constant_memory': True}}) as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
    except Exception:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

@st.cache_data(show_spinner=False)
def convert_df_to_csv(df):
    """Convert dataframe to CSV - much faster and lighter for raw data. Cached."""
    return df.to_csv(index=False).encode('utf-8')

def create_download_button(df, filename, button_text="📥 Download Excel", is_csv=False):
    """
    Create a download button. 
    Uses cached conversion functions to avoid RAM spikes and session_state bloating.
    """
    if df is None:
        return

    if is_csv:
        data = convert_df_to_csv(df)
        mime = "text/csv"
    else:
        data = convert_df_to_excel(df)
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    st.download_button(
        label=button_text,
        data=data,
        file_name=filename,
        mime=mime,
        use_container_width=True
    )


# Main App
st.markdown('<h1 class="main-header">📊 Sales vs Return Data Analyzer</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Upload your data files to generate comprehensive reports</p>', unsafe_allow_html=True)

# Initialize session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
    st.session_state.results = {}

# Initialize session state for lazy download generation
if 'zip_data' not in st.session_state:
    st.session_state.zip_data = None

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
        "Upload Purchase Master Excel",
        type=['xlsx', 'xls'],
        key='product_master'
    )
    
# Process Button
st.markdown("---")
process_button = st.button("🚀 Process Data", use_container_width=True, type="primary")

if process_button:
    # Reset lazy download states when reprocessing
    st.session_state.raw_excel_data = None
    st.session_state.zip_data = None
    
    if not (b2b_files or b2c_files):
        st.error("Please upload at least one B2B or B2C report file.")
    else:
        with st.spinner("Processing your data..."):
            try:
                # Combine zip files
                progress_text = st.empty()
                progress_text.text("📚 Combining zip files...")
                all_zip_files = (b2b_files or []) + (b2c_files or [])
                combined_df = read_zip_files(all_zip_files)
                
                # Store raw count and raw unfiltered df before any processing
                raw_total_records = len(combined_df)
                raw_combined_df = combined_df.copy()

                if combined_df.empty:
                    st.error("No data found in the uploaded files.")
                else:
                    # Process combined data
                    progress_text.text("🔍 Filtering shipment transactions...")
                    combined_df = process_combined_data(combined_df)
                    
                    progress_text.text("🛡️ Cleaning data for display...")
                    combined_df = ensure_arrow_compatibility(combined_df)
                    # Skip compatibility check for raw_combined_df to save time (we only need it for CSV download)

                    # Load product master
                    if product_master_file:
                        progress_text.text("📂 Loading Purchase Master...")
                        pm_df = pd.read_excel(product_master_file)
                        progress_text.text("🔗 Merging Purchase details...")
                        combined_df = merge_product_master(combined_df, pm_df)
                        
                    # Create pivots
                    progress_text.text("📊 Creating analysis pivots...")
                    brand_qty_pivot = create_brand_pivot(combined_df)
                    asin_qty_pivot = create_asin_pivot(combined_df)
                    
                    # Process Seller Flex
                    seller_flex_df = None
                    seller_flex_brand = None
                    seller_flex_asin = None
                    
                    if seller_flex_file and product_master_file:
                        progress_text.text("📦 Processing Seller Flex data...")
                        seller_flex_df = pd.read_csv(seller_flex_file)
                        seller_flex_df = process_seller_flex(seller_flex_df, pm_df)
                        seller_flex_df = ensure_arrow_compatibility(seller_flex_df)
                        
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
                    fba_disposition_pivot = None
                    fba_disposition_brand_pivot = None
                    
                    if fba_return_file and product_master_file:
                        fba_return_df = pd.read_csv(fba_return_file)
                        fba_return_df = process_fba_return(fba_return_df, pm_df)
                        fba_return_df = ensure_arrow_compatibility(fba_return_df)

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
                        
                        # Create ASIN x Disposition pivot table
                        if "detailed-disposition" in fba_return_df.columns:
                            fba_disposition_pivot = fba_return_df.pivot_table(
                                index="asin",
                                columns="detailed-disposition",
                                values="quantity",
                                aggfunc="sum",
                                fill_value=0
                            ).reset_index()
                            # Add total column
                            fba_disposition_pivot["Total"] = fba_disposition_pivot.select_dtypes(include='number').sum(axis=1)
                            fba_disposition_pivot = fba_disposition_pivot.sort_values("Total", ascending=False)
                            
                            # Create Brand x Disposition pivot table
                            fba_disposition_brand_pivot = fba_return_df.pivot_table(
                                index="Brand",
                                columns="detailed-disposition",
                                values="quantity",
                                aggfunc="sum",
                                fill_value=0
                            ).reset_index()
                            
                            # We don't need a Total column here immediately as we calculate it in summary, 
                            # but filtering numeric cols is safer if we did.
                    
                    # Create final summaries
                    progress_text.text("📝 Generating final summaries...")
                    if fba_return_brand is not None and seller_flex_brand is not None:
                        brand_final = create_final_summary(
                            brand_qty_pivot,
                            fba_return_brand,
                            seller_flex_brand,
                            fba_disposition_brand_pivot
                        )
                    else:
                        brand_final = brand_qty_pivot
                    
                    # Create ASIN final summary with returns
                    if fba_return_asin is not None or seller_flex_asin is not None:
                        asin_final = create_asin_final_summary(
                            asin_qty_pivot,
                            fba_return_asin,
                            seller_flex_asin,
                            pm_df if product_master_file else None,
                            fba_disposition_pivot
                        )
                    else:
                        asin_final = asin_qty_pivot
                    
                    # Calculate metrics before adding Grand Totals
                    total_records = len(combined_df)
                    total_brands = len(brand_qty_pivot)
                    total_asins = len(asin_qty_pivot)
                    total_sf_returns = len(seller_flex_df) if seller_flex_df is not None else 0

                    # Add Grand Totals to all dataframes
                    combined_df = add_grand_total(combined_df)
                    brand_qty_pivot = add_grand_total(brand_qty_pivot)
                    asin_qty_pivot = add_grand_total(asin_qty_pivot)
                    brand_final = add_grand_total(brand_final)
                    asin_final = add_grand_total(asin_final)
                    
                    if seller_flex_df is not None:
                        seller_flex_df = add_grand_total(seller_flex_df)
                    if seller_flex_brand is not None:
                        seller_flex_brand = add_grand_total(seller_flex_brand)
                    if seller_flex_asin is not None:
                        seller_flex_asin = add_grand_total(seller_flex_asin)
                    
                    if fba_return_df is not None:
                        fba_return_df = add_grand_total(fba_return_df)
                    if fba_return_brand is not None:
                        fba_return_brand = add_grand_total(fba_return_brand)
                    if fba_return_asin is not None:
                        fba_return_asin = add_grand_total(fba_return_asin)
                    if fba_disposition_pivot is not None:
                        fba_disposition_pivot = add_grand_total(fba_disposition_pivot)

                    # Aggressive cleanup of local references
                    gc.collect()

                    # Store results
                    # combined_df is stored because it's needed for analysis tabs.
                    # raw_combined_df is stored because user requested raw data download.
                    st.session_state.results = {
                        'combined_df': combined_df,
                        'raw_combined_df': raw_combined_df,
                        'brand_qty_pivot': brand_qty_pivot,
                        'asin_qty_pivot': asin_qty_pivot,
                        'asin_final': asin_final,
                        'seller_flex_df': seller_flex_df,
                        'seller_flex_brand': seller_flex_brand,
                        'seller_flex_asin': seller_flex_asin,
                        'fba_return_df': fba_return_df,
                        'fba_return_brand': fba_return_brand,
                        'fba_return_asin': fba_return_asin,
                        'fba_disposition_pivot': fba_disposition_pivot,
                        'brand_final': brand_final,
                        'metrics': {
                            'total_records': int(total_records),
                            'raw_total_records': int(raw_total_records),
                            'total_brands': int(total_brands),
                            'total_asins': int(total_asins),
                            'total_sf_returns': int(total_sf_returns)
                        }
                    }
                    
                    st.session_state.processed = True
                    st.success("✅ Data processed successfully!")
                    
                    # Final cleanup of local references
                    gc.collect()
            except Exception as e:
                st.error(f"An error occurred during processing: {str(e)}")
                st.code(traceback.format_exc())

# Display Results
if st.session_state.processed:
    try:
        st.markdown("---")
        st.markdown("## 📊 Analysis Results")
        
        results = st.session_state.results
    
        
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            metrics = results.get('metrics', {})
            st.metric("Total Records (Raw)", f"{metrics.get('raw_total_records', 0):,}")
            st.metric("Filtered Records", f"{metrics.get('total_records', 0):,}")
        with col2:
            st.metric("Total Brands", f"{metrics.get('total_brands', 0):,}")
        with col3:
            st.metric("Total ASINs", f"{metrics.get('total_asins', 0):,}")
        with col4:
            st.metric("Seller Flex Returns", f"{metrics.get('total_sf_returns', 0):,}")
        
        # Tabs for different reports
        tab_raw, tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Raw Combined Data",
            "Filtered Data",
            "Brand Analysis",
            "ASIN Analysis",
            "Seller Flex",
            "FBA Returns"
        ])

        with tab_raw:
            st.subheader("Raw Unfiltered Combined Data")
            raw_count = results.get('metrics', {}).get('raw_total_records', 0)
            st.info(f"This report contains all {raw_count:,} records without any filtering.")
            
            if 'raw_combined_df' in results and results['raw_combined_df'] is not None:
                create_download_button(
                    results['raw_combined_df'], 
                    "raw_combined_unfiltered_report.csv", 
                    "📥 Download Raw Unfiltered CSV", 
                    is_csv=True
                )
                st.caption("Tip: CSV format is recommended for large raw datasets.")
            else:
                st.warning("Raw combined data not available.")
        with tab1:
            st.subheader("Filtered Transaction Data (Shipments Only)")
            st.dataframe(results['combined_df'].head(100), use_container_width=True)
            create_download_button(results['combined_df'], "filtered_shipment_report.xlsx", "📥 Download Filtered Excel")
            create_download_button(results['combined_df'], "filtered_shipment_report.csv", "📥 Download Filtered CSV", is_csv=True)
        
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
                st.subheader("Raw Seller Flex Data")
                st.dataframe(results['seller_flex_df'].head(100), use_container_width=True)
                create_download_button(results['seller_flex_df'], "seller_flex_raw_data.xlsx")
                
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
                st.subheader("Raw FBA Return Data")
                st.dataframe(results['fba_return_df'].head(100), use_container_width=True)
                create_download_button(results['fba_return_df'], "fba_return_raw_data.xlsx")

                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("FBA Return - Brand Pivot")
                    st.dataframe(results['fba_return_brand'], use_container_width=True)
                    create_download_button(results['fba_return_brand'], "fba_return_brand.xlsx")
                
                with col2:
                    st.subheader("FBA Return - ASIN Pivot")
                    st.dataframe(results['fba_return_asin'], use_container_width=True)
                    create_download_button(results['fba_return_asin'], "fba_return_asin.xlsx")
                
                # FBA Disposition Pivot Table
                if results.get('fba_disposition_pivot') is not None:
                    st.subheader("FBA Return - ASIN x Disposition Pivot")
                    st.dataframe(results['fba_disposition_pivot'], use_container_width=True)
                    create_download_button(results['fba_disposition_pivot'], "fba_disposition_pivot.xlsx")
            else:
                st.info("No FBA Return data uploaded")
        
        # Download All Button
        st.markdown("---")
        st.subheader("📥 Download All Reports")
        
        # Use session state to persist ZIP data across reruns
        if st.button("🛠️ Generate Reports ZIP (Analysis Only)", use_container_width=True, key="gen_zip"):
            with st.spinner("Creating ZIP file..."):
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    # Essential reports - generate Excel bytes on the fly (cached)
                    essential_dfs = {
                        'brand_final': results.get('brand_final'),
                        'asin_final': results.get('asin_final'),
                        'brand_qty_pivot': results.get('brand_qty_pivot'),
                        'asin_qty_pivot': results.get('asin_qty_pivot'),
                        'seller_flex_brand': results.get('seller_flex_brand'),
                        'seller_flex_asin': results.get('seller_flex_asin'),
                        'fba_return_brand': results.get('fba_return_brand'),
                        'fba_return_asin': results.get('fba_return_asin'),
                        'fba_disposition': results.get('fba_disposition_pivot')
                    }
                    
                    for name, df in essential_dfs.items():
                        if df is not None:
                            # Use cached conversion function to save memory
                            zip_file.writestr(f"{name}.xlsx", convert_df_to_excel(df))
                
                st.session_state.zip_data = zip_buffer.getvalue()
                gc.collect()
        
        # Show download button if ZIP is ready (persists across reruns)
        if st.session_state.zip_data is not None:
            st.download_button(
                label="📦 Download Analysis ZIP",
                data=st.session_state.zip_data,
                file_name="amazon_analysis_reports.zip",
                mime="application/zip",
                use_container_width=True,
                key="dl_zip"
            )

    except Exception as e:
        st.error(f"An error occurred during display: {str(e)}")
        st.code(traceback.format_exc())

# Footer
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #6b7280; padding: 2rem;'>
        <p>Upload your B2B/B2C reports, Seller Flex data, FBA returns, and Purchase Master to generate comprehensive analytics</p>
        <p style='font-size: 0.875rem;'>Supported formats: ZIP (B2B/B2C), CSV (Seller Flex, FBA Return), XLSX (Purchase Master)</p>
    </div>
""", unsafe_allow_html=True)

