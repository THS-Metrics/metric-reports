import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from database.ms_sql_connection import fetch_query
from utils.utils import update_dashboard
from environment.settings import config
from utils.utils import update_dashboard, combined_df, filter_last_12_months, make_archive_copy
from datetime import datetime
import calendar


def adult_extraction(year: int, month: int):
  reference_date=f'{year}-{month:02}-01'
  #Returns Last date of the month in the format YYYY-MM-DD
  last_date=datetime.strptime(f'{year}-{month:02}-{calendar.monthrange(year, month)[1]}', '%Y-%m-%d')
  #Reference date
  reference_date=f'{year}-{month:02}-01'
  print(reference_date, last_date)
  
  adult_query= f"""
    WITH SurgeryMap AS (
        SELECT *
        FROM (VALUES
            ('Orchidectomy, cryptorchid', 'Neuter'),
            ('Orchidectomy',              'Neuter'),
            ('Ovariohysterectomy',         'Spay'),
            ('Dental Extraction',          'Dental'),
            ('Dental COHAT (Lv 1-3)',      'Dental'),
            ('Dental COHAT (Lv 4-5)',      'Dental'),
            ('COHAT',                      'Dental'),
            ('Dental Extraction, Difficult','Dental'),
            ('Dental Dehiscence Repair',   'Dental')
        ) v(Medication, SurgeryCategory)
    ),
    VisitRank AS (
        SELECT
            et.ExamID,
            v.AnimalID,
            v.tin_DateCreated,
            v.IntakeType,
            ROW_NUMBER() OVER (
                PARTITION BY et.ExamID
                ORDER BY v.tin_DateCreated DESC
            ) AS rn
        FROM ExamTreatment et
        JOIN txnVisit v
            ON v.AnimalID = et.AnimalID
          AND v.tin_DateCreated < et.StatusDateTime
        WHERE v.tin_DateCreated >= '2019-01-01'
          AND v.IntakeType IN ('OwnerSurrender', 'TransferIn', 'Stray', '[Return]')
    ),

    cte as(SELECT
        a.AnimalID,
        a.Name,
        ad.DateOfBirth,
        rs.Species,
        vr.IntakeType,
        vr.tin_DateCreated AS IntakeDate,
        et.Medication,
        et.ExamID AS SurgeryID,
        sm.SurgeryCategory,
        et.StatusDateTime AS SurgeryDate,
        ras.Stage,
        hs.LastUpdated AS StageDate,
        DATEDIFF(DAY, vr.tin_DateCreated, et.StatusDateTime) AS SurgeryWait,
        DATEDIFF(WEEK, ad.DateOfBirth, vr.tin_DateCreated) AS IntakeAge
    FROM Animal a
    JOIN AnimalDetails ad ON ad.AnimalID = a.AnimalID
    JOIN refSpecies rs ON rs.SpeciesID = a.SpeciesID
    JOIN ExamTreatment et ON et.AnimalID = a.AnimalID
    JOIN VisitRank vr ON vr.ExamID = et.ExamID AND vr.rn = 1
    JOIN SurgeryMap sm ON sm.Medication = et.Medication
    JOIN HistoryStatus hs ON hs.AnimalID = a.AnimalID
    JOIN refAnimalStage ras ON ras.StageID = hs.StageID
    WHERE
        rs.Species IN ('Cat', 'Dog', 'Rabbit')
        AND ras.Stage IN ('Evaluate', 'Post -Op', 'Surgery Needed')
        AND hs.LastUpdated >= '2019-01-01'
        AND hs.LastUpdated <  '{last_date}'
        AND et.StatusDateTime >= '{reference_date}'
        AND et.StatusDateTime <  '{last_date}'),

    first_cleaned
      AS (SELECT
        animalid,
        name,
        species,
        dateofbirth,
        intakedate,
        intaketype,
        medication,
        surgerycategory,
        surgerydate,
        stage,
        StageDate,
        IntakeAge,
        DATEDIFF(DAY, Intakedate, SurgeryDate) AS SurgeryWait,
        '7 wks+' AS AgeGroup,

        --Partition by animalid and assign row number to first item
        --Helps us to pick 'surgery needed' as first followed by  'post op' and  'evaluate'
        ROW_NUMBER() OVER (PARTITION BY animalid, surgerycategory ORDER BY stage DESC) AS rn
      FROM cte

      WHERE surgerycategory IS NOT NULL
      AND intakeage >= 7
      --IF looking at post-op stages only
      AND Stage <> 'Surgery Needed')
      --Cats
      SELECT
        AnimalID,
        Name,
        Species,
        DateofBirth,
        IntakeDate,
        IntakeType,
        Medication,
        Surgerycategory,
        SurgeryDate,
        Stage,
        StageDate,
        IntakeAge,
        SurgeryWait,
        Agegroup,
        --Inserting goals for each month. Average in pivot table gives us expected monthly goal
        CASE
          WHEN SurgeryCategory = 'Spay' THEN 8
          WHEN SurgeryCategory = 'Neuter' THEN 7
          WHEN SurgeryCategory = 'Dental' THEN 14

        END AS Sx_goal
      FROM first_cleaned
      WHERE rn = 1
      AND species = 'Cat'

      UNION ALL
      --Dogs
      SELECT
        AnimalID,
        Name,
        Species,
        DateofBirth,
        IntakeDate,
        IntakeType,
        Medication,
        Surgerycategory,
        SurgeryDate,
        Stage,
        StageDate,
        IntakeAge,
        SurgeryWait,
        Agegroup,
        --Inserting goals for each month. Average in pivot table gives us expected monthly goal
        CASE
          WHEN SurgeryCategory = 'Spay' THEN 8
          WHEN SurgeryCategory = 'Neuter' THEN 7
          WHEN SurgeryCategory = 'Dental' THEN 14

        END AS Sx_goal
      FROM first_cleaned
      WHERE rn = 1
      AND species = 'Dog'

      UNION ALL
      --SpecialSpecies
      SELECT
        AnimalID,
        Name,
        Species,
        DateofBirth,
        IntakeDate,
        IntakeType,
        Medication,
        Surgerycategory,
        SurgeryDate,
        Stage,
        StageDate,
        IntakeAge,
        SurgeryWait,
        Agegroup,
        CASE
          WHEN SurgeryCategory = 'Spay' THEN 8
          WHEN SurgeryCategory = 'Neuter' THEN 7
          WHEN SurgeryCategory = 'Dental' THEN 14

        END AS Sx_goal
      FROM first_cleaned
      WHERE rn = 1
      AND species = 'Rabbit'
      """

  df = fetch_query(adult_query)

  
  return df

def parse_combined_df(adult_extract_function, start_year, end_year) -> pd.DataFrame:
  """Combines the dataframes for each year and months into a single dataframe"""
  df=combined_df(adult_extract_function, start_year, end_year)
  df['SurgeryDate'] = pd.to_datetime(df['SurgeryDate']).dt.date

  # Corrected filtering with parentheses and proper types
  min_SurgeryDate = df['SurgeryDate'].min()
  max_SurgeryDate = pd.Timestamp.today().normalize()
  print(max_SurgeryDate)
  SurgeryDate_range = pd.date_range(min_SurgeryDate, max_SurgeryDate, freq='MS')

  # Generating the missing rows
  missing_rows = []
  for Surgerycategory in df['Surgerycategory'].unique():
      for Species in df['Species'].unique():
          existing_SurgeryDates = df[df['Surgerycategory'] == Surgerycategory]['SurgeryDate']
          missing_SurgeryDates = set(SurgeryDate_range) - set(existing_SurgeryDates)
          for SurgeryDate in missing_SurgeryDates:
              missing_rows.append({'SurgeryDate': SurgeryDate, 'Surgerycategory': Surgerycategory, 'Species': Species})
  # Convert missing rows to DataFrame before concatenating
  missing_df = pd.DataFrame(missing_rows)
  # Appending the missing rows to the DataFrame
  df = pd.concat([df,missing_df], ignore_index=True)

  # Sorting the DataFrame by SurgeryDate and Surgerycategory
  df = df.sort_values(by=['AnimalID']).reset_index(drop=True)
  df['<10']=np.where(df['SurgeryWait']< 10, 1, 0)
  df['10-20']=np.where((df['SurgeryWait'] >= 10) & (df['SurgeryWait'] <= 20), 1, 0)
  df['>20']=np.where(df['SurgeryWait']> 20, 1, 0)
  #Fill Null Dental with Dental goal
  df.loc[df['Surgerycategory']=='Dental', 'Sx_goal']=14
  df.loc[df['Surgerycategory']=='Spay', 'Sx_goal']=8
  df.loc[df['Surgerycategory']=='Neuter', 'Sx_goal']=7

  return df


def filter_current_year_data(df, year: str):
    # Ensure SurgeryDate is datetime
    df["SurgeryDate"] = pd.to_datetime(df["SurgeryDate"], errors="coerce")

    # Convert year string to integer (e.g., "2024" -> 2024)
    year = int(year)

    # Define date range for the whole year
    start_date = pd.Timestamp(year=year, month=1, day=1)
    end_date   = pd.Timestamp(year=year + 1, month=1, day=1)

    # Filter rows within the year
    mask = (df["SurgeryDate"] >= start_date) & (df["SurgeryDate"] < end_date)

    return df.loc[mask]



def run_sx_wait_time_report(report_year: int, report_month: int):
    """Generate sx wait time report and archive files by year/month"""
    
    # --- Parse 3 years of data ---
    yearly = parse_combined_df(adult_extract_function=adult_extraction, start_year=report_year-2, end_year=report_year)

    # --- Define file paths ---
    bi_report_filename = "sx_wait_time_bi_report.xlsx"
    bi_report_path = f"{config.SERVER_PATH}/power_bi/{bi_report_filename}"

    report_filename = "sx_report.xlsx"
    report_path = f"{config.SERVER_PATH}/sx_wait_time/{report_filename}"

    yearly_filename = "sx_yearly_report.xlsx"
    yearly_path = f"{config.SERVER_PATH}/sx_wait_time/{yearly_filename}"

    dashboard_filename = "sx_DashBoard.xlsx"
    dashboard_path = f"{config.SERVER_PATH}/sx_wait_time/{dashboard_filename}"

    # --- Save yearly report ---
    yearly.to_excel(yearly_path, index=False)

    # --- Filter and save current year data ---
    adult_df = filter_current_year_data(yearly, report_year)
    adult_df.to_excel(report_path, header=True, index=False, sheet_name='Adult')

    # --- Generate BI data ---
    bi_data = filter_last_12_months(yearly, report_year, "SurgeryDate")
    bi_data.to_excel(bi_report_path, index=False)

    # --- Update dashboard ---
    update_dashboard(dashboard_path)

    # --- Archive copies with year/month appended ---
    make_archive_copy(
        report_year,
        report_month,
        base_path=f"{config.SERVER_PATH}/sx_wait_time",
        paths_to_copy=[yearly_path, report_path, dashboard_path]
    )

    return




