import pandas as pd
from database.ms_sql_connection import fetch_query
from utils.utils import update_dashboard, combined_df, make_archive_copy
from environment.settings import config
from datetime import datetime
import calendar


def los_outcome_script(year: int, month: int) -> pd.DataFrame:
    #Generates report date as previous
    reference_date=f'{year}-{month:02}-01'
    last_date=datetime.strptime(f'{year}-{month:02}-{calendar.monthrange(year, month)[1]}', '%Y-%m-%d')
    
    
    query=f"""
        -- ========================================
        -- REPORT: Total Pets that Came in Previous Month
        -- ========================================

        

        WITH month_intake AS (
            SELECT 
                a.AnimalID,
                a.Name,
                s.Species,
                CAST(v.tIn_DateCreated AS DATE) AS IntakeDate,
                CAST(v.tOut_DateCreated AS DATE) AS OutcomeDate,
                COALESCE(v.OutComeType, 'No Outcome Yet') AS Outcome
            FROM Animal a
            JOIN refSpecies s ON a.SpeciesID = s.SpeciesID
            LEFT JOIN txnVisit v ON v.AnimalID = a.AnimalID
            WHERE 
                (v.IntakeType IN ('TransferIn', 'OwnerSurrender', '[Return]', 'Stray') OR v.IntakeType IS NULL)
                AND v.tIn_DateCreated >= '{reference_date}'
                AND v.tIn_DateCreated <= '{last_date}'
        ),

        with_outcome AS (
            SELECT 
                AnimalID, Name, Species, IntakeDate, OutcomeDate, Outcome
            FROM month_intake
            WHERE OutcomeDate >= '{reference_date}' AND OutcomeDate <= '{last_date}'
        ),

        latest_stagedate AS (
            SELECT
                hs.AnimalID,
                hs.Status,
                MAX(hs.LastUpdated) AS StageDate
            FROM HistoryStatus hs
            WHERE hs.LastUpdated > DATEADD(YEAR, -5, '{last_date}')
              AND hs.LastUpdated <= '{last_date}'
            GROUP BY hs.AnimalID, hs.Status
        ),

        past_intakes AS (
            SELECT
                v.AnimalID,
                MAX(v.tIn_DateCreated) AS IntakeDate
            FROM txnVisit v
            WHERE v.tIn_DateCreated IS NOT NULL
              AND v.tIn_DateCreated < '{reference_date}'
            GROUP BY v.AnimalID
        ),

        inventory_outcomed AS (
            SELECT 
                ls.AnimalID,
                a.Name,
                s.Species,
                CAST(pi.IntakeDate AS DATE) AS IntakeDate,
                CAST(v.tOut_DateCreated AS DATE) AS OutcomeDate,
                ls.StageDate,
                v.OutComeType AS Outcome
            FROM latest_stagedate ls
            JOIN Animal a ON ls.AnimalID = a.AnimalID
            JOIN refSpecies s ON a.SpeciesID = s.SpeciesID
            JOIN past_intakes pi ON pi.AnimalID = ls.AnimalID
            LEFT JOIN txnVisit v ON v.AnimalID = a.AnimalID
            WHERE 
                v.tOut_DateCreated >= '{reference_date}'
                AND v.tOut_DateCreated <= '{last_date}'
                AND v.OutComeType IN ('PreEuthanasia', 'Adoption', 'Died', 'ReturnToOwner', 'TransferOut')
                AND ls.Status = 'I'
        ),

        outcomed AS (
            SELECT AnimalID, Name, Species, IntakeDate, OutcomeDate, Outcome
            FROM inventory_outcomed
            UNION ALL
            SELECT AnimalID, Name, Species, IntakeDate, OutcomeDate, Outcome
            FROM with_outcome        
        ),

        total_outcomed AS (
            SELECT 
                AnimalID,
                Name,
                Species,
                MAX(IntakeDate) as IntakeDate,
                MAX(OutcomeDate) AS OutcomeDate,
                Outcome
            FROM outcomed
            WHERE Species IN ('Cat', 'Dog') AND OutcomeDate IS NOT NULL
            AND OutcomeDate >= IntakeDate
            GROUP BY AnimalID, Name, Species, Outcome 
        ),

        location_history AS (
            SELECT 
                hl.AnimalID,
                rl.Location,
                hl.LastUpdated,
                DATEDIFF(DAY, hl.LastUpdated, 
                        LEAD(hl.LastUpdated) OVER (PARTITION BY hl.AnimalID ORDER BY hl.LastUpdated)) AS FosterDays
            FROM HistoryLocation hl
            JOIN refLocations rl ON hl.LocationID = rl.LocationID
            JOIN total_outcomed t ON t.AnimalID = hl.AnimalID   
            WHERE hl.LastUpdated > t.IntakeDate --only fetch history between intake and last date of report
            and hl.LastUpdated <= '{last_date}'
        ),

        foster_period_outcomed AS (
            SELECT DISTINCT lh.AnimalID, lh.LastUpdated, lh.FosterDays
            FROM location_history lh
            JOIN total_outcomed t ON t.AnimalID = lh.AnimalID
            WHERE lh.FosterDays IS NOT NULL AND lh.Location='Fosters'
        ),

        foster_sum_outcomed AS (
            SELECT AnimalID, SUM(FosterDays) AS FosterDays
            FROM foster_period_outcomed
            GROUP BY AnimalID
        )

        SELECT DISTINCT
            t.AnimalID,
            t.Name,
            t.Species,
            t.IntakeDate,
            t.OutcomeDate,
            COALESCE(f.FosterDays, 0) AS DaysInFoster,
            DATEDIFF(DAY, t.IntakeDate, t.OutcomeDate) AS TotalLOS,
            DATEDIFF(DAY, t.IntakeDate, t.OutcomeDate) - COALESCE(f.FosterDays, 0) AS DaysInShelter,
            'Outcomed' AS OutcomeType,
            '{last_date}' AS ReportDate,
            CASE 
                WHEN MONTH(t.IntakeDate) = MONTH('{last_date}') THEN 'New Intake'
                ELSE 'Already in Shelter'
            END AS Type
        FROM total_outcomed t
        LEFT JOIN foster_sum_outcomed f ON t.AnimalID = f.AnimalID
        order by AnimalID;
        """

    df=fetch_query(query)
    return df
    

def los_nonoutcome_script(year: int, month: int) -> pd.DataFrame:
    #fetches current date
    reference_date=f'{year}-{month:02}-01'
    last_date=datetime.strptime(f'{year}-{month:02}-{calendar.monthrange(year, month)[1]}', '%Y-%m-%d')
    
    
    query=f"""

    -- ========================================
    -- REPORT: Total Pets that Came in Previous Month
    -- ========================================

    WITH month_intake AS (
        SELECT 
            a.AnimalID,
            a.Name,
            s.Species,
            CAST(v.tIn_DateCreated AS DATE) AS IntakeDate,
            v.tOut_DateCreated AS OutcomeDate,
            COALESCE(v.OutComeType, 'No Outcome Yet') AS Outcome
        FROM Animal a
        JOIN refSpecies s ON a.SpeciesID = s.SpeciesID
        LEFT JOIN txnVisit v ON v.AnimalID = a.AnimalID
        WHERE 
            (v.IntakeType IN ('TransferIn', 'OwnerSurrender', '[Return]', 'Stray') OR v.IntakeType IS NULL)
            AND v.tIn_DateCreated >= '{reference_date}'
            AND v.tIn_DateCreated <= '{last_date}'
            AND Species IN ('Cat', 'Dog')
    ),
    /* Keep only those whose outcome is AFTER the report date OR NULL (i.e., still no outcome as of report date) */
    without_outcome AS (
        SELECT 
            AnimalID, Name, Species, IntakeDate, OutcomeDate, Outcome
        FROM month_intake
        WHERE OutcomeDate > '{last_date}'
        OR OutcomeDate is NULL
    ),

    latest_stagedate AS (
        SELECT
            hs.AnimalID,
            MAX(hs.LastUpdated) AS StageDate
        FROM HistoryStatus hs
        WHERE hs.LastUpdated > DATEADD(YEAR, -5, '{last_date}')
        AND hs.LastUpdated <= '{last_date}'
        GROUP BY hs.AnimalID
    ),

    past_intakes AS (
        SELECT
            v.AnimalID,
            MAX(v.tIn_DateCreated) AS IntakeDate
        FROM txnVisit v
        WHERE v.tIn_DateCreated IS NOT NULL
        AND v.tIn_DateCreated < '{reference_date}'
        GROUP BY v.AnimalID
    ),
    /* Keep only those whose outcome is AFTER the report date OR NULL (i.e., still no outcome as of report date) */
    inventory_nonoutcomed AS (
        SELECT 
            ls.AnimalID,
            a.Name,
            s.Species,
            CAST(pi.IntakeDate AS DATE) AS IntakeDate,
            CAST(v.tOut_DateCreated AS DATE) AS OutcomeDate,
            ls.StageDate,
            v.OutComeType AS Outcome,
            status
        FROM latest_stagedate ls
        JOIN Animal a ON ls.AnimalID = a.AnimalID
        JOIN refSpecies s ON a.SpeciesID = s.SpeciesID
        join HistoryStatus
            on HistoryStatus.LastUpdated=ls.stagedate
        JOIN past_intakes pi ON pi.AnimalID = ls.AnimalID
        LEFT JOIN txnVisit v ON v.AnimalID = a.AnimalID
        WHERE v.tOut_DateCreated > '{last_date}'
            AND Species IN ('Cat', 'Dog')
    ),

    nonoutcomed AS (
        SELECT 
            inv.AnimalID,
            inv.Name,
            inv.Species,
            CAST(inv.IntakeDate AS DATE) AS IntakeDate,
            '{last_date}' AS Outcomedate  -- Default Outcomedate for non outcomed animals
        FROM inventory_nonoutcomed inv
        WHERE Status = 'A'

        UNION ALL

        SELECT
            AnimalID,
            Name,
            Species,
            CAST(IntakeDate AS DATE) AS IntakeDate,
            '{last_date}' AS Outcomedate  -- Default Outcomedate for non outcomed animals
        FROM without_outcome   
    ),
    total_nonoutcomed AS
    (SELECT 
            AnimalID,
            Name,
            Species,
            MAX(IntakeDate) as IntakeDate,
            Outcomedate
            from nonoutcomed
            where Outcomedate >= IntakeDate
            GROUP BY AnimalID, Name, Species, Outcomedate
            ),
    
    location_history AS (
        SELECT 
            hl.AnimalID,
            rl.Location,
            hl.LastUpdated,
            DATEDIFF(DAY, hl.LastUpdated, 
                    LEAD(hl.LastUpdated) OVER (PARTITION BY hl.AnimalID ORDER BY hl.LastUpdated)) AS FosterDays
        FROM HistoryLocation hl
        JOIN refLocations rl ON hl.LocationID = rl.LocationID
        JOIN total_nonoutcomed t ON t.AnimalID = hl.AnimalID   
        WHERE hl.LastUpdated > t.IntakeDate --only fetch history between intake and outcome date
        and hl.LastUpdated <= t.Outcomedate
    ),

    foster_period_non_outcomed AS (
        SELECT DISTINCT lh.AnimalID, lh.LastUpdated, COALESCE(lh.FosterDays, DATEDIFF(DAY, lh.LastUpdated, '{last_date}')) as FosterDays
        FROM location_history lh
        WHERE lh.Location='Fosters'
    ),

    foster_sum_nonoutcomed AS (
        SELECT AnimalID, SUM(FosterDays) AS FosterDays
        FROM foster_period_non_outcomed
        GROUP BY AnimalID
    )

    SELECT DISTINCT
        t.AnimalID,
        t.Name,
        t.Species,
        t.IntakeDate,
        '{last_date}' as Outcomedate,
        COALESCE(f.FosterDays, 0) AS DaysInFoster,
        DATEDIFF(DAY, t.IntakeDate, t.OutcomeDate) AS TotalLOS,
        DATEDIFF(DAY, t.IntakeDate, t.OutcomeDate) - COALESCE(f.FosterDays, 0) AS DaysInShelter,
        'Outcomed' AS OutcomeType,
        '{last_date}' AS ReportDate,
        CASE 
            WHEN MONTH(t.IntakeDate) = MONTH('{last_date}') THEN 'New Intake'
            ELSE 'Already in Shelter'
        END AS Type
    FROM total_nonoutcomed t
    LEFT JOIN foster_sum_nonoutcomed f ON t.AnimalID = f.AnimalID
    order by AnimalID;
	
    """
    df=fetch_query(query)
    return df

def parse_combined_df(los_function, start_year, end_year) -> pd.DataFrame:
  """Combines the dataframes for each year and months into a single dataframe"""
  df=combined_df(los_function, start_year, end_year)
  df["ReportDate"]=df["ReportDate"].apply(pd.to_datetime)
  df["IntakeDate"]=df["IntakeDate"].apply(pd.to_datetime)

  return df    

def normalize_excel_data_columns(df: pd.DataFrame):
    #renaming due to datamodel on excel dashboard
    df = df.rename(columns={
    "AnimalID": "animalid",
    "Name": "name",
    "Species": "species",
    "IntakeDate": "intakedate",
    "Outcomedate": "outcomedate",
    "DaysInFoster": "days_in_foster",
    "TotalLOS": "total_los",
    "DaysInShelter": "days_in_shelter",
    "OutcomeType": "OutcomeType",   
    "ReportDate": "Reportdate",
    "Type": "Type"                 
})
    return df


def run_los_report(report_year: int, report_month: int):
    """Generate LOS report and archive files by year/month"""
    
    # --- Parse data ---
    los_outcome_data = parse_combined_df(los_outcome_script, report_year, report_year)
    los_nonoutcome_data = parse_combined_df(los_nonoutcome_script, report_year, report_year)

    # --- Define file paths ---
    outcome_filename = "los_outcome_data.xlsx"
    outcome_path = f"{config.SERVER_PATH}/los_in_shelter/{outcome_filename}"

    non_outcome_filename = "los_nonoutcome_data.xlsx"
    non_outcome_path = f"{config.SERVER_PATH}/los_in_shelter/{non_outcome_filename}"

    dashboard_filename = "los_shelter_dashboard.xlsx"
    dashboard_path = f"{config.SERVER_PATH}/los_in_shelter/{dashboard_filename}"

    # --- Save normalized data ---
    normalize_excel_data_columns(los_outcome_data).to_excel(outcome_path, index=False)
    normalize_excel_data_columns(los_nonoutcome_data).to_excel(non_outcome_path, index=False)

    # --- Update dashboard ---
    update_dashboard(dashboard_path)

    # --- Archive copies with year/month appended ---
    make_archive_copy(
        report_year,
        report_month,
        base_path=f"{config.SERVER_PATH}/los_in_shelter",
        paths_to_copy=[outcome_path, non_outcome_path, dashboard_path]
    )

    return
