import win32com.client as win32
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
import calendar
import shutil
import os

def update_dashboard(dashboard_path:str):
    """Updates dashboard, adds Numerator and Denominator data to Dashboard Sheet"""
    excel=win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(dashboard_path)
    #Refresh Dashboard with new data
    wb.RefreshAll()
    wb.Save()
    excel.Application.Quit()
    return

def save_to_excel(numerator, denominator, path: str) -> None :
    '''Saves dataframe to Server and autofits columns'''
    #Saves Sheets to Excel
    writer = pd.ExcelWriter(path, engine = 'xlsxwriter')
    denominator.to_excel(writer, header=True, index=False, sheet_name = 'Denominator')
    numerator.to_excel(writer, header=True, index=False, sheet_name = 'Numerator')
    writer.close()  
    #Autofit Column width
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    wb = excel.Workbooks.Open(path)
    denom = wb.Worksheets("Denominator")
    num= wb.Worksheets("Numerator")
    denom.Columns.AutoFit()
    num.Columns.AutoFit()
    wb.Save()
    excel.Application.Quit()
    print("Report file saved to server.")
    return

def combined_df(fetch_fn, start_year: int, end_year: int) -> pd.DataFrame:
    """
    Build a combined DataFrame for all available months and years using a data-fetching function.

    Args:
        fetch_fn (callable): Function that takes (year, month) and returns a pandas DataFrame.
        start_year (int): First year (inclusive).
        end_year (int): Last year (inclusive).

    Returns:
        pd.DataFrame: Combined DataFrame for all valid (year, month) combinations.
    """
    frames = []
    current_year = datetime.today().year
    current_month = datetime.today().month

    for year in range(start_year, end_year + 1):
        if year>current_year:
            break
        for month in range(1, 13):
            
            # Skip future months of the current year
            if year == current_year and month > current_month:
                break

            # Handle December report (when current_month == January)
            if current_month == 1 and year == current_year:
                for prev_month in range(1, 13):
                    print(year-1, prev_month)
                    frames.append(fetch_fn(year - 1, prev_month))
                    print(f"First Month case Data extracted for year: {year-1}, month: {prev_month}")
                break  # after filling December data, stop for this year
            else: 
                # Normal case
                print(year, month)
                frames.append(fetch_fn(year, month))
                print(f"Normal Case Data extracted for year: {year}, month: {month}")
    return pd.concat(frames, ignore_index=True)


def filter_last_12_months(df:pd.DataFrame, year:str, filter_key:str):
    """
    Filters the DataFrame to include only rows where ReferenceDate is within the last 6 months.
    Also ensures missing ReferenceDate months are filled in with zero values for visualization.

    Parameters:
        df (pd.DataFrame): Input DataFrame with 'ReferenceDate' column.

    Returns:
        pd.DataFrame: Filtered DataFrame with missing months populated.
    """
    year=int(year)
    # Ensure ReferenceDate is in datetime format
    df[filter_key] = pd.to_datetime(df[filter_key])
    # Get today's date normalized to midnight
    max_date   = pd.Timestamp(year=year+1, month=1, day=1)

    # Start date = first day of the month 12 months ago
    start_date = max_date - relativedelta(months=13)

    # Filter SurgeryDate between start_date (inclusive) and max_date (exclusive)
    mask = (df[filter_key] >= start_date) & (df[filter_key] < max_date)
    df = df.loc[mask]


    return df

def make_archive_copy(report_year: int, report_month: int, base_path: str, paths_to_copy: list):
    """Copy files to year/month folder and append year/month to filenames"""
    
    # --- Create archive folder with month name ---
    month_name = calendar.month_name[report_month]  # e.g., 2 -> "February"
    archive_folder = os.path.join(base_path, str(report_year), month_name)
    os.makedirs(archive_folder, exist_ok=True)
    
    # --- Copy each file with year_month appended ---
    for file_path in paths_to_copy:
        if os.path.isfile(file_path):
            # Extract original filename and extension
            filename, ext = os.path.splitext(os.path.basename(file_path))
            # New filename: originalname_YYYY_MM.ext
            new_filename = f"{filename}_{report_year}_{str(report_month).zfill(2)}{ext}"
            # Destination path
            dest_path = os.path.join(archive_folder, new_filename)
            shutil.copy(file_path, dest_path)
