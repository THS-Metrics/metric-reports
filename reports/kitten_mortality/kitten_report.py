import pandas as pd
from datetime import datetime
import calendar
import numpy as np
from dateutil.relativedelta import relativedelta
from utils.utils import save_to_excel, update_dashboard, combined_df, make_archive_copy, truncate_report_to_data_month
from database.ms_sql_connection import fetch_query
from environment.settings import config



def extraction(year: int, month: int) -> pd.DataFrame:
    """Extraction and Transformation script from SQL"""
    #Returns Last day of the month
    #Returns Last date of the month in the format YYYY-MM-DD
    first_date=f"{year}-{month:02}-01"
    last_date=datetime.strptime(f'{year}-{month:02}-{calendar.monthrange(year, month)[1]}', '%Y-%m-%d')
    last_four_months_date = last_date - relativedelta(months=5)
    
    query=f'''WITH Denom AS (SELECT  
    dbo.Animal.AnimalID, 
    dbo.Animal.Name, 
    dbo.txnVisit.IntakeType as IntakeType,
    cast(dbo.AnimalDetails.DateOfBirth as date) as DateOfBirth,
    cast(dbo.txnVisit.tin_DateCreated as Date) AS IntakeDate, 
    dbo.txnVisit.tOut_DateCreated,
    DATEDIFF(week, DateOfBirth, dbo.txnVisit.tin_DateCreated ) as IntakeAge

    FROM  dbo.Animal INNER JOIN
           dbo.refSpecies ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID INNER JOIN
           dbo.AnimalDetails ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID INNER JOIN
           dbo.txnVisit ON dbo.Animal.AnimalID = dbo.txnVisit.AnimalID
    WHERE dbo.refSpecies.Species = 'Cat'
    --Using last_date to include Cats that had intake date in specified month
    AND dbo.txnVisit.tin_DateCreated Between '{last_four_months_date}' AND '{last_date}'
    AND dbo.txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender')
    --Excludes animals that had outcome before after report month
    AND dbo.txnVisit.tOut_DateCreated >= '{last_date}'),
    
    --Weeks Table from inbuilt table in SQL Server for Crossjoin 
    Weeks AS (
    SELECT DATEADD(DAY, number,'{last_date}') AS week_start
    FROM master..spt_values
    WHERE type = 'P' AND number in(0,7,14,21,27)
    ),
    
    Numerator as (
    SELECT DISTINCT
      dbo.Animal.AnimalID AS animalid,
      dbo.Animal.Name,
      dbo.refSpecies.Species,
      CAST(dbo.AnimalDetails.DateOfBirth AS date) AS DateOfBirth,
      dbo.txnVisit.IntakeType,
      dbo.txnVisit.IntakeSubType,
      CAST(dbo.txnVisit.tin_DateCreated AS date) AS IntakeDate,
      CASE
        WHEN dbo.txnVisit.OutComeType = 'PreEuthanasia' THEN 'Euthanised'
        WHEN dbo.txnVisit.OutComeType = 'Died' THEN 'Died'
      END AS OutcomeType,
      CAST(dbo.txnVisit.tOut_DateCreated AS date) AS OutcomeDate
    FROM dbo.Animal
    INNER JOIN dbo.refSpecies
      ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID
    INNER JOIN dbo.AnimalDetails
      ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID
    INNER JOIN dbo.txnVisit
      ON dbo.Animal.AnimalID = dbo.txnVisit.AnimalID
    WHERE (dbo.refSpecies.Species = 'Cat')
    AND (dbo.txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND dbo.AnimalDetails.DateOfBirth IS NOT NULL
    AND (dbo.txnVisit.OutComeType IN ('Died', 'PreEuthanasia'))
    AND dbo.txnVisit.tin_DateCreated Between '{last_four_months_date}' AND '{last_date}'
    
    UNION
    
    SELECT DISTINCT
      dbo.Animal.AnimalID,
      dbo.Animal.Name,
      dbo.refSpecies.Species,
      CAST(dbo.AnimalDetails.DateOfBirth AS date) AS DateOfBirth,
      dbo.txnVisit.IntakeType,
      dbo.txnVisit.IntakeSubType,
      CAST(dbo.txnVisit.tin_DateCreated AS date) AS IntakeDate,
      'Euthanised' AS OutcomeType,
      CAST(dbo.Euthanasia.DateCreated AS date) AS EuthDate
    FROM dbo.Animal
    INNER JOIN dbo.AnimalDetails
      ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID
    INNER JOIN dbo.refSpecies
      ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID
    INNER JOIN dbo.txnVisit
      ON dbo.Animal.AnimalID = dbo.txnVisit.AnimalID
    INNER JOIN dbo.Euthanasia
      ON dbo.Animal.AnimalID = dbo.Euthanasia.AnimalID
    WHERE (dbo.refSpecies.Species = 'Cat')
    AND (dbo.txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND (dbo.txnVisit.tin_DateCreated >= '2017-01-01 ')
    AND (dbo.txnVisit.tin_DateCreated < GETDATE())
    AND dbo.AnimalDetails.DateOfBirth IS NOT NULL
    --Exclude AnimlID in first numerator table
    AND dbo.Animal.AnimalID NOT IN (SELECT DISTINCT
      dbo.Animal.AnimalID
    FROM dbo.Animal
    INNER JOIN dbo.refSpecies
      ON dbo.Animal.SpeciesID = dbo.refSpecies.SpeciesID
    INNER JOIN dbo.AnimalDetails
      ON dbo.Animal.AnimalID = dbo.AnimalDetails.AnimalID
    INNER JOIN dbo.txnVisit
      ON dbo.Animal.AnimalID = dbo.txnVisit.AnimalID
    WHERE dbo.refSpecies.Species = 'Cat'
    AND dbo.txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender')
    AND dbo.txnVisit.OutComeType IN ('Died', 'PreEuthanasia')
    AND dbo.txnVisit.tin_DateCreated Between '{last_four_months_date}' AND '{last_date}'))


    SELECT DISTINCT
      Animalid,
      Name,
      IntakeType,
      Dateofbirth,
      Intakedate,
      DATEDIFF(WEEK, dateofbirth, intakedate) AS IntakeAge,
      Cast('{last_date}' as Date) as ReferenceDate,
      'Alive' AS Outcometype,
      CASE
        WHEN DATEDIFF(WEEK, DAteOfBirth, week_start) BETWEEN 0 AND 2 THEN '0-2 wks'
        WHEN DATEDIFF(WEEK, DAteOfBirth, week_start) BETWEEN 3 AND 6 THEN '3-6 wks'
        WHEN DATEDIFF(WEEK, DAteOfBirth, week_start) BETWEEN 7 AND 12 THEN '7-12 wks'
        WHEN DATEDIFF(WEEK, DAteOfBirth, week_start) BETWEEN 13 AND 20 THEN '13-20 wks'
      END AS Agegroup

    FROM Denom 
    CROSS JOIN Weeks 
    
    WHERE DateofBirth IS NOT NULL
    AND DATEDIFF(WEEK, DateOfBirth, week_start) BETWEEN 0 AND 20
    AND intakeage BETWEEN 0 AND 20
    AND intakedate <= week_start
    AND DATEDIFF(WEEK, week_start, tOut_DateCreated) >= 0
    AND AnimalID NOT IN
    --excludes kittens that died in previous weeks
    (SELECT
    Animalid
    FROM Numerator
    WHERE DATEDIFF(WEEK, DateOfBirth, OutcomeDate) <= 20
    AND outcomedate < week_start)
   
    --combines numerator rows with denominator
    --Union takes care of duplicates
    UNION
    
    SELECT
      Animalid,
      Name,
      IntakeType,
      Dateofbirth,
      Intakedate,
      DATEDIFF(WEEK, dateofbirth, intakedate) AS IntakeAge,
      Outcomedate,
      Outcometype,
      CASE
        WHEN DATEDIFF(WEEK, DateOfBirth, OutcomeDate) BETWEEN 0 AND 2 THEN '0-2 wks'
        WHEN DATEDIFF(WEEK, DateOfBirth, OutcomeDate) BETWEEN 3 AND 6 THEN '3-6 wks'
        WHEN DATEDIFF(WEEK, DateOfBirth, OutcomeDate) BETWEEN 7 AND 12 THEN '7-12 wks'
        WHEN DATEDIFF(WEEK, DAteOfBirth, OutcomeDate) BETWEEN 13 AND 20 THEN '13-20 wks'
      END AS Agegroup

    FROM numerator
    WHERE DATEDIFF(WEEK, DateOfBirth, OutcomeDate) <= 20
    AND (outcomedate >='{first_date}' AND outcomedate<='{last_date}')
    
    '''
    
    #converts query to dataframe
    df=fetch_query(query)
    #changes strings to datetime dtypes
    df[["Dateofbirth", "Intakedate", "ReferenceDate"]] = df[["Dateofbirth", "Intakedate", "ReferenceDate"]].apply(
        lambda x: pd.to_datetime(x).dt.date)
    #Remove occurences where there are 2 same agegroups for outcome type
    #E.g same agegroup for a kitten died and alive thus causing duplicate
    #Deletes Alive row for such occurences
    df=df.drop_duplicates(subset=['Animalid','Agegroup'], keep='last')

    return df

    
    
def parse_combined_df(function_name:str, start_year:int, end_year:int, max_month:int) -> pd.DataFrame:
    """Combines the dataframes for each year and months into a single dataframe"""
    df=combined_df(function_name, start_year, end_year, max_month=max_month)
    # Creating a referencedate range with the missing months
    min_referencedate = df['ReferenceDate'].min()
    max_referencedate = df['ReferenceDate'].max()
    print(max_referencedate, min_referencedate)
    referencedate_range = pd.date_range(min_referencedate, max_referencedate, freq='MS')
    # Generating the missing rows for months
    missing_rows = []
    unique_age_groups=["0-2 wks", "3-6 wks", "7-12 wks", "13-20 wks"]
    for AgeGroup in unique_age_groups:
      existing_referencedates = df[df['Agegroup'] == AgeGroup]['ReferenceDate']
      missing_referencedates = set(referencedate_range) - set(existing_referencedates)
      for referencedate in missing_referencedates:
          for missing_outcome in ["Alive", "Died", "Euthanised"]:
            missing_rows.append({'ReferenceDate': referencedate, 'Agegroup': AgeGroup, "Outcometype": missing_outcome})
    # Convert missing rows to DataFrame before concatenating
    missing_df = pd.DataFrame(missing_rows)
    # Appending the missing rows to the DataFrame
    df = pd.concat([df, missing_df], ignore_index=True)
    #Sorting the DataFrame by referencedate and AgeGroup
    df = df.sort_values(by=['Animalid']).reset_index(drop=True)
    
    return df


def filter_last_twelve_months(df:pd.DataFrame, year:str, month:str):
    """
    Filters the DataFrame to include only rows where ReferenceDate is within the last 6 months.
    Also ensures missing ReferenceDate months are filled in with zero values for visualization.

    Parameters:
        df (pd.DataFrame): Input DataFrame with 'ReferenceDate' column.

    Returns:
        pd.DataFrame: Filtered DataFrame with missing months populated.
    """
    # Ensure ReferenceDate is in datetime format
    df = df.copy()  # Avoid modifying the original DataFrame
    df["ReferenceDate"] = pd.to_datetime(df["ReferenceDate"])

   
    max_date = pd.Timestamp(year=year, month=int(month), day=1)

    # Start date = first day of the month 12 months ago
    start_date = max_date - relativedelta(months=12)

    # Filter SurgeryDate between start_date (inclusive) and max_date (exclusive)
    mask = (df["ReferenceDate"] >= start_date) & (df["ReferenceDate"] < max_date)
    df = df.loc[mask]

    # Add a count column
    df["sum"] = 1
    df.loc[df["Animalid"].isna(), "sum"] = 0
    df["Month"] = df["ReferenceDate"].dt.to_period("M")
    return df

def process_bi_data(df):
    # Replace NaN in 'sum' where 'Animalid' is missing
    df.loc[df["Animalid"].isna(), "sum"] = 0

    # Subset relevant columns
    df = df[["ReferenceDate", "Outcometype", "Agegroup", "sum"]].copy()

    # Classify outcomes into 'alive' and 'deceased'
    df["outcome_refined"] = np.where(df["Outcometype"] == "Alive", "alive", "deceased")

    # Normalize 'ReferenceDate' and extract monthly period
    df["ReferenceDate"] = pd.to_datetime(df["ReferenceDate"])
    df["Month"] = df["ReferenceDate"].dt.to_period("M")

    # Segment dataset by age group categories
    df_above_2 = df[df["Agegroup"].isin(["7-12 wks", "3-6 wks"])]
    df_below_2 = df[df["Agegroup"] == "0-2 wks"]

    # Internal function to compute deceased percentage and assign category label
    def compute_percentage(df_subset, category_label):
      outcome_summary = (
          df_subset.groupby(["Month", "outcome_refined"])["sum"]
          .sum()
          .unstack(fill_value=0)
      )

      # Ensure both columns exist
      for col in ["deceased", "alive"]:
          if col not in outcome_summary.columns:
              outcome_summary[col] = 0

      # Calculate deceased percentage safely
      total = outcome_summary["deceased"] + outcome_summary["alive"]
      outcome_summary["deceased_pct"] = np.where(total == 0, 0, (outcome_summary["deceased"] / total) * 100)

      result = outcome_summary.reset_index()[["Month", "deceased_pct"]]
      result["AgeGroupCategory"] = category_label
      return result

    # Apply percentage calculation for both age groups
    below_2_result = compute_percentage(df_below_2, "0-2 wks")
    above_2_result = compute_percentage(df_above_2, "3-12 wks")

    # Concatenate results with category label
    final_result = pd.concat([below_2_result, above_2_result], ignore_index=True)
    final_result["deceased_pct"] = final_result["deceased_pct"].round(2)

    return final_result



def run_kitten_report(report_year: int, report_month: int, run_bi_data: bool = True):
    """Generate kitten mortality report and archive files by year/month"""

    base_path = f"{config.SERVER_PATH}/kitten_mortality"
    bi_base_path = f"{config.SERVER_PATH}/power_bi"

    # --- Load and truncate data ---
    df = parse_combined_df(extraction, report_year, report_year, report_month)
    df = truncate_report_to_data_month(df, report_year, report_month, filter_key="ReferenceDate")

    # --- File paths ---
    files = {
        "report": f"{base_path}/kitten_mortality.xlsx",
        "dashboard": f"{base_path}/kitten_mortality_dashboard_template.xlsx",
        "bi_report": f"{bi_base_path}/kitten_mortality_bi.xlsx",
    }

    # --- Mortality subset ---
    df_num = df[df["Outcometype"].isin(["Died", "Euthanised"])]

    # --- Optional Power BI dataset ---
    if run_bi_data:
        two_year_df = parse_combined_df(extraction, report_year - 1, report_year, report_month)
        two_year_df = truncate_report_to_data_month(
            two_year_df, report_year, report_month, filter_key="ReferenceDate"
        )

        power_bi_df = filter_last_twelve_months(two_year_df, report_year, report_month)
        power_bi_df_summary = process_bi_data(power_bi_df)

        with pd.ExcelWriter(files["bi_report"], engine="openpyxl") as writer:
            power_bi_df.to_excel(writer, sheet_name="Sheet2", index=False)
            power_bi_df_summary.to_excel(writer, sheet_name="Sheet1", index=False)

    # --- Save mortality report ---
    save_to_excel(df_num, df, files["report"])

    # --- Update dashboard ---
    update_dashboard(files["dashboard"])

    # --- Archive copies ---
    make_archive_copy(
        report_year,
        report_month,
        base_path=base_path,
        paths_to_copy=[files["report"], files["dashboard"]],
    )
    print(f"Kitten Mortality report generation for {report_year}-{report_month:02d} completed.")



