import win32com.client as win32
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
import calendar
import shutil
import os
import asyncio
import pythoncom
from pandas.tseries.offsets import MonthEnd


def update_dashboard(dashboard_path: str):
    """Updates dashboard and refreshes Excel dashboard"""

    pythoncom.CoInitialize()

    excel = None
    wb = None

    try:
        excel = win32.Dispatch("Excel.Application")

        # Optional: hide Excel, but skip if not allowed
        try:
            excel.Visible = False
        except Exception:
            pass

        wb = excel.Workbooks.Open(dashboard_path)

        wb.RefreshAll()
        excel.CalculateUntilAsyncQueriesDone()

        wb.Save()
        wb.Close(SaveChanges=True)

        print("Dashboard refreshed successfully.")

    except Exception as e:
        print("Error updating dashboard:", e)

    finally:
        try:
            if wb:
                wb.Close(SaveChanges=False)
        except:
            pass

        try:
            if excel:
                excel.Quit()
        except:
            pass

        pythoncom.CoUninitialize()



def save_to_excel(numerator, denominator, path: str):

    writer = pd.ExcelWriter(path, engine='xlsxwriter')

    denominator.to_excel(writer, index=False, sheet_name='Denominator')
    numerator.to_excel(writer, index=False, sheet_name='Numerator')

    workbook = writer.book
    denom_ws = writer.sheets['Denominator']
    num_ws = writer.sheets['Numerator']

    for i, col in enumerate(denominator.columns):
        width = max(denominator[col].astype(str).map(len).max(), len(col)) + 2
        denom_ws.set_column(i, i, width)

    for i, col in enumerate(numerator.columns):
        width = max(numerator[col].astype(str).map(len).max(), len(col)) + 2
        num_ws.set_column(i, i, width)

    writer.close()

    print("Report file saved to server.")

async def combined_df_async(fetch_fn, start_year: int, end_year: int) -> pd.DataFrame:
    """
    Build a combined DataFrame for all available months and years using a data-fetching function.
    """

    tasks = []
    current_year = datetime.today().year
    current_month = datetime.today().month

    for year in range(start_year, end_year + 1):
        if year > current_year:
            break

        for month in range(1, 13):

            # Skip future months
            if year == current_year and month > current_month:
                break

            # Special January case
            if current_month == 1 and year == current_year:
                for prev_month in range(1, 13):
                    print(year - 1, prev_month)
                    tasks.append(
                        asyncio.to_thread(fetch_fn, year - 1, prev_month)
                    )
                break
            else:
                print(year, month)
                tasks.append(
                    asyncio.to_thread(fetch_fn, year, month)
                )

    # Run everything concurrently
    frames = await asyncio.gather(*tasks)

    return pd.concat(frames, ignore_index=True)

def combined_df(fetch_fn, start_year: int, end_year: int, max_month:int=12) -> pd.DataFrame:
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
        for month in range(1, max_month+1):
            
            # Skip future months of the current year
            if year == current_year and month > current_month:
                break
            # Normal case
            print(year, month)
            frames.append(fetch_fn(year, month))
            print(f"Data extracted for year: {year}, month: {month}")
                
    return pd.concat(frames, ignore_index=True)


def filter_last_12_months(df:pd.DataFrame, year:str, month:str,  filter_key:str):
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

    max_date   = pd.Timestamp(year=year, month=int(month), day=1)

    # Start date = first day of the month 12 months ago
    start_date = max_date - relativedelta(months=12)

    # Filter SurgeryDate between start_date (inclusive) and max_date (exclusive)
    mask = (df[filter_key] >= start_date) & (df[filter_key] < max_date)
    df = df.loc[mask]


    return df

def make_archive_copy(report_year: int, report_month: int, base_path: str, paths_to_copy: list):
    """Copy files to year/month folder, rename if mapped, and append year/month to filenames"""

    # --- Create archive folder with month name ---
    month_name = calendar.month_name[report_month]  # e.g., 2 -> "February"
    archive_folder = os.path.join(base_path, "data_archives", str(report_year), month_name)
    dashboard_archive_folder= os.path.join(base_path, "dashboard_archives", str(report_year), month_name)
    os.makedirs(archive_folder, exist_ok=True)
    os.makedirs(dashboard_archive_folder, exist_ok=True)

    # Mapping of old_name to new_name
    filename_mapping = {
        "kitten_mortality_dashboard.xlsx": "Kitten_Mortality.xlsx",
        "uri_report_dashboard.xlsx": "URI.xlsx",
        "surgicalcomplicationsdashboard.xlsx": "Surgical_Complications_Shelter.xlsx",
        "sx_dashboard.xlsx": "Surgery_Wait_Times.xlsx",
        "ringworm_report_dashboard.xlsx": "Ringworm.xlsx",
        "parvovirus_report_dashboard.xlsx": "Parvo.xlsx",
        "los_shelter_dashboard.xlsx": "Los_Shelter.xlsx",
        "diarrhea_report.xlsx": "Diarrhea.xlsx"
    }

    # --- Copy each file ---
    for file_path in paths_to_copy:
        if not os.path.isfile(file_path):
            continue

        original_filename = os.path.basename(file_path)

        # --- Rename if in mapping ---
        copy_to_dashboard_archive= False
        print(original_filename)
        if original_filename.lower() in filename_mapping:
            print(original_filename.lower())
            print("found")
            final_base_name = filename_mapping[original_filename.lower()]
            copy_to_dashboard_archive=True
        else:
            final_base_name = original_filename
            

        # Split name and extension
        filename, ext = os.path.splitext(final_base_name)

        # Append year and month
        new_filename = f"{filename}_{report_year}_{str(report_month).zfill(2)}{ext}"

        # Destination path
        dest_path = os.path.join(archive_folder, new_filename)

        shutil.copy(file_path, dest_path)

        # --- Copy to dashboard archive if mapped ---
        if copy_to_dashboard_archive:
            dashboard_dest_path = os.path.join(dashboard_archive_folder, new_filename)
            shutil.copy(file_path, dashboard_dest_path)


def truncate_report_to_data_month(df: pd.DataFrame, year: str, month: str, filter_key: str):
    """
    Filters the DataFrame to include only rows where filter_key is between
    the first day of the year and the last day of the specified month.
    """
    df = df.copy()
    df[filter_key] = pd.to_datetime(df[filter_key])

    # First day of year
    start_date = pd.Timestamp(year=int(year), month=1, day=1)

    # Last day of specified month
    end_date = pd.Timestamp(year=int(year), month=int(month), day=1) + MonthEnd(0)

    print(f"start date {start_date}")
    print(f"end date {end_date}")

    # Filter rows
    filtered_df = df[(df[filter_key] >= start_date) & (df[filter_key] <= end_date)]

    return filtered_df