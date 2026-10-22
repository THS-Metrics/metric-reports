import pandas as pd
from database.ms_sql_connection import fetch_query
from utils.utils import (
    update_dashboard,
    combined_df,
    make_archive_copy,
    truncate_report_to_data_month,
)
from environment.settings import config
from datetime import datetime
import calendar
import numpy as np


def los_outcome_script(year: int, month: int) -> pd.DataFrame:
    # Generates report date as previous
    reference_date = f"{year}-{month:02}-01"
    last_date = datetime.strptime(
        f"{year}-{month:02}-{calendar.monthrange(year, month)[1]}", "%Y-%m-%d"
    )

    query = f"""
        -- ========================================
        -- REPORT: Total Pets that Came in Previous Month
        -- ========================================

        WITH month_intake AS (
            SELECT 
                a.AnimalID,
                a.Name,
                s.Species,
                CAST(v.tIn_DateCreated AS DATE) AS IntakeDate,
                CAST(COALESCE(v.tOut_DateCreated, ad.ReleaseDate) AS DATE) AS OutcomeDate,
                COALESCE(v.OutComeType, 'No Outcome Yet') AS Outcome
            FROM Animal a
            JOIN refSpecies s ON a.SpeciesID = s.SpeciesID
            LEFT JOIN txnVisit v ON v.AnimalID = a.AnimalID
            LEFT JOIN Adoption ad ON ad.AnimalID = a.AnimalID
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
            JOIN refAnimalStage rs
                on rs.StageID=hs.StageID
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
            AND (v.IntakeType IN ('TransferIn', 'OwnerSurrender', '[Return]', 'Stray') OR v.IntakeType IS NULL)
              AND v.tIn_DateCreated < '{reference_date}'
            GROUP BY v.AnimalID
        ),

        inventory_outcomed AS (
            SELECT 
                ls.AnimalID,
                a.Name,
                s.Species,
                CAST(pi.IntakeDate AS DATE) AS IntakeDate,
                CAST(COALESCE(v.tOut_DateCreated, ad.ReleaseDate) AS DATE) AS OutcomeDate,
                ls.StageDate,
                v.OutComeType AS Outcome
            FROM latest_stagedate ls
            JOIN Animal a ON ls.AnimalID = a.AnimalID
            JOIN refSpecies s ON a.SpeciesID = s.SpeciesID
            JOIN past_intakes pi ON pi.AnimalID = ls.AnimalID
            LEFT JOIN txnVisit v ON v.AnimalID = a.AnimalID
            LEFT JOIN Adoption ad ON ad.AnimalID = a.AnimalID
            WHERE 
                COALESCE(v.tOut_DateCreated, ad.ReleaseDate) >= '{reference_date}'
                AND COALESCE(v.tOut_DateCreated, ad.ReleaseDate) <= '{last_date}'
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
            WHERE OutcomeDate IS NOT NULL
            AND OutcomeDate >= IntakeDate
            GROUP BY AnimalID, Name, Species, Outcome 
        ),

        location_history AS (
            SELECT 
                hl.AnimalID,
                rl.Location,
                hl.LastUpdated,
                DATEDIFF(
                        DAY,
                        hl.LastUpdated,
                        COALESCE(
                            LEAD(hl.LastUpdated) OVER (PARTITION BY hl.AnimalID ORDER BY hl.LastUpdated),
                            t.Outcomedate
                        )
                    ) AS DaysInLocation
                    FROM HistoryLocation hl
            JOIN refLocations rl ON hl.LocationID = rl.LocationID
            JOIN total_outcomed t ON t.AnimalID = hl.AnimalID   
            WHERE hl.LastUpdated > t.IntakeDate --only fetch history between intake and last date of report
            and hl.LastUpdated <= '{last_date}'
        ),
        
        current_location_days AS (
            SELECT 
                cl.AnimalID,
                rl.Location,
                cl.LastUpdated,
                DATEDIFF(DAY, cl.LastUpdated, t.Outcomedate) AS DaysInLocation
            FROM CurrentLocation cl
            JOIN refLocations rl 
                ON cl.LocationID = rl.LocationID
            JOIN total_outcomed t 
                ON t.AnimalID = cl.AnimalID
            WHERE cl.LastUpdated <= t.Outcomedate
        ),

        -- 🔑 Combine both history + current
        all_location_periods AS (
            SELECT AnimalID, Location, LastUpdated, DaysInLocation
            FROM location_history

            UNION ALL

            SELECT AnimalID, Location, LastUpdated, DaysInLocation
            FROM current_location_days
        ),

        foster_period_outcomed AS (
            SELECT 
                AnimalID,
                LastUpdated,
                DaysInLocation AS FosterDays
            FROM all_location_periods
            WHERE Location = 'Fosters'
        ),

        foster_sum_outcomed AS (
            SELECT 
                AnimalID, 
                SUM(FosterDays) AS FosterDays
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
        WHERE  EXISTS (
            SELECT 1 
            FROM Chip c
            WHERE c.AnimalID = t.AnimalID
        )
        order by AnimalID;
        """

    df = fetch_query(query)
    return df


def los_nonoutcome_script(year: int, month: int) -> pd.DataFrame:
    # fetches current date
    reference_date = f"{year}-{month:02}-01"
    last_date = datetime.strptime(
        f"{year}-{month:02}-{calendar.monthrange(year, month)[1]}", "%Y-%m-%d"
    )

    query = f"""

    -- ========================================
    -- REPORT: Total Pets that Came in Previous Month
    -- ========================================

    WITH month_intake AS (
        SELECT 
            a.AnimalID,
            a.Name,
            s.Species,
            CAST(v.tIn_DateCreated AS DATE) AS IntakeDate,
            v.IntakeType, 
            CAST(COALESCE(v.tOut_DateCreated, ad.ReleaseDate) AS DATE) AS OutcomeDate,
            COALESCE(v.OutComeType, 'No Outcome Yet') AS Outcome
        FROM Animal a
        JOIN refSpecies s ON a.SpeciesID = s.SpeciesID
        LEFT JOIN txnVisit v ON v.AnimalID = a.AnimalID
        LEFT JOIN Adoption ad ON ad.AnimalID = a.AnimalID
        WHERE 
             v.tIn_DateCreated >= '{reference_date}'
            AND v.tIn_DateCreated <= '{last_date}'
    ),
    /* Keep only those whose outcome is AFTER the report date OR NULL (i.e., still no outcome as of report date) */
    without_outcome AS (
        SELECT 
            AnimalID, Name, Species, IntakeDate, IntakeType, OutcomeDate, Outcome
        FROM month_intake
        WHERE OutcomeDate > '{last_date}'
        OR OutcomeDate is NULL
    ),

    latest_stagedate AS (
        SELECT
            hs.AnimalID,
            hs.LastUpdated AS StageDate,
            rs.Stage,
            hs.Status,
            ROW_NUMBER() OVER (
                PARTITION BY hs.AnimalID
                ORDER BY hs.LastUpdated DESC
            ) AS rn
        FROM HistoryStatus hs
        JOIN refAnimalStage rs
            ON rs.StageID = hs.StageID
        WHERE hs.LastUpdated > DATEADD(YEAR, -5, '{last_date}')
        AND hs.LastUpdated <= '{last_date}'
    ),

    past_intakes AS (
        SELECT
            v.AnimalID,
            v.tIn_DateCreated AS IntakeDate,
            v.IntakeType,
            ROW_NUMBER() OVER (
                PARTITION BY v.AnimalID
                ORDER BY v.tIn_DateCreated DESC
            ) AS rn
        FROM txnVisit v
        WHERE v.tIn_DateCreated IS NOT NULL
        AND v.tIn_DateCreated < '{reference_date}'
    ),
    past_non_outcome_date AS (
    SELECT
    pi.AnimalID,
    MAX(
        CASE
            WHEN COALESCE(v.tOut_DateCreated, ad.ReleaseDate) > pi.IntakeDate
            THEN CAST(COALESCE(v.tOut_DateCreated, ad.ReleaseDate) AS DATE)
        END
    ) AS OutcomeDate
        FROM past_intakes pi
        LEFT JOIN txnVisit v
            ON v.AnimalID = pi.AnimalID
        LEFT JOIN Adoption ad
            ON ad.AnimalID = pi.AnimalID
        GROUP BY
            pi.AnimalID
    ),
    /* Keep only those whose outcome is AFTER the report date OR NULL (i.e., still no outcome as of report date) */
    inventory_nonoutcomed AS (
        SELECT
            ls.AnimalID,
            a.Name,
            s.Species,
            CAST(pi.IntakeDate AS DATE) AS IntakeDate,
            pi.IntakeType,
            CAST(COALESCE(pd.OutcomeDate, '{last_date}') AS DATE) AS OutcomeDate,
            ls.StageDate,
            ls.Stage,
            ls.Status
        FROM latest_stagedate ls
        JOIN Animal a
            ON a.AnimalID = ls.AnimalID
        JOIN refSpecies s
            ON s.SpeciesID = a.SpeciesID
        JOIN past_intakes pi
            ON pi.AnimalID = ls.AnimalID
        AND pi.rn = 1
        LEFT JOIN past_non_outcome_date pd
            ON pd.AnimalID = ls.AnimalID
        WHERE ls.rn = 1
    ),

    nonoutcomed AS (
        SELECT 
            inv.AnimalID,
            inv.Name,
            inv.Species,
            CAST(inv.IntakeDate AS DATE) AS IntakeDate,
            inv.IntakeType, 
            '{last_date}' AS Outcomedate  -- Default Outcomedate for non outcomed animals
        FROM inventory_nonoutcomed inv
        WHERE Status = 'A'

        UNION ALL

        SELECT
            AnimalID,
            Name,
            Species,
            CAST(IntakeDate AS DATE) AS IntakeDate,
            IntakeType,
            '{last_date}' AS Outcomedate  -- Default Outcomedate for non outcomed animals
        FROM without_outcome   
    ),
    nonoutcomed_ranked AS (
        SELECT
            n.*,
            ROW_NUMBER() OVER (
                PARTITION BY n.AnimalID
                ORDER BY n.IntakeDate DESC
            ) AS rn
        FROM nonoutcomed n
        WHERE n.Outcomedate >= n.IntakeDate
        AND (
            n.IntakeType IN (
                'TransferIn',
                'OwnerSurrender',
                '[Return]',
                'Stray'
            )
            OR n.IntakeType IS NULL
        )
    ),

    total_nonoutcomed AS (
        SELECT
            AnimalID,
            Name,
            Species,
            IntakeDate,
            IntakeType,
            Outcomedate
        FROM nonoutcomed_ranked
        WHERE rn = 1
    ),
    
    location_history AS (
        SELECT 
            hl.AnimalID,
            rl.Location,
            hl.LastUpdated,
            DATEDIFF(
                        DAY,
                        hl.LastUpdated,
                        COALESCE(
                            LEAD(hl.LastUpdated) OVER (PARTITION BY hl.AnimalID ORDER BY hl.LastUpdated),
                            t.Outcomedate
                        )
                    ) AS DaysInLocation
                FROM HistoryLocation hl
        JOIN refLocations rl ON hl.LocationID = rl.LocationID
        JOIN total_nonoutcomed t ON t.AnimalID = hl.AnimalID   
        WHERE hl.LastUpdated > t.IntakeDate --only fetch history between intake and outcome date
        and hl.LastUpdated <= t.Outcomedate
    ),
    current_location_days AS (
        SELECT 
            cl.LastUpdated,
            cl.AnimalID,
            rl.Location,
            t.Outcomedate,
            DATEDIFF(DAY, cl.LastUpdated, t.Outcomedate) AS DaysInLocation
        FROM CurrentLocation cl
        JOIN refLocations rl 
            ON cl.LocationID = rl.LocationID
        JOIN total_nonoutcomed t 
            ON t.AnimalID = cl.AnimalID
        WHERE cl.LastUpdated <= t.Outcomedate
    ),

    -- 🔑 Combine both history + current
    all_location_periods AS (
        SELECT AnimalID, Location, LastUpdated, DaysInLocation
        FROM location_history

        UNION ALL

        SELECT AnimalID, Location, LastUpdated, DaysInLocation
        FROM current_location_days
    ),

    foster_period_non_outcomed AS (
        SELECT 
            AnimalID,
            LastUpdated,
            DaysInLocation AS FosterDays
        FROM all_location_periods
        WHERE Location = 'Fosters'
    ),

    foster_sum_nonoutcomed AS (
        SELECT 
            AnimalID, 
            SUM(FosterDays) AS FosterDays
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
        'NonOutcomed' AS OutcomeType,
        '{last_date}' AS ReportDate,
        CASE 
            WHEN MONTH(t.IntakeDate) = MONTH('{last_date}') THEN 'New Intake'
            ELSE 'Already in Shelter'
        END AS Type
    FROM total_nonoutcomed t
    LEFT JOIN foster_sum_nonoutcomed f ON t.AnimalID = f.AnimalID
    WHERE  EXISTS (
        SELECT 1 
        FROM Chip c
        WHERE c.AnimalID = t.AnimalID
    )
    order by AnimalID;
	
    """
    df = fetch_query(query)
    return df


def parse_combined_df(los_function, start_year, end_year, report_month) -> pd.DataFrame:
    """Combines the dataframes for each year and months into a single dataframe"""
    df = combined_df(los_function, start_year, end_year, max_month=report_month)
    df["ReportDate"] = df["ReportDate"].apply(pd.to_datetime)
    df["IntakeDate"] = df["IntakeDate"].apply(pd.to_datetime)
    df["FosterStatus"] = np.where(
        df["DaysInFoster"] > 0, "has_foster_history", "no_foster_history"
    )
    df = df[df["DaysInShelter"] >= 0].copy()
    df["DaysInFoster"] = np.where(df["DaysInFoster"] <= 0, np.nan, df["DaysInFoster"])

    return df


def normalize_excel_data_columns(df: pd.DataFrame):
    # renaming due to datamodel on excel dashboard
    df = df.rename(
        columns={
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
            "Type": "Type",
            "FosterStatus": "foster_status",
        }
    )
    return df


def run_los_report(report_year: int, report_month: int):
    """Generate LOS report and archive files by year/month"""

    # --- Parse data ---
    los_outcome_data = parse_combined_df(
        los_outcome_script, report_year, report_year, report_month
    )
    los_nonoutcome_data = parse_combined_df(
        los_nonoutcome_script, report_year, report_year, report_month
    )
    los_outcome_data = truncate_report_to_data_month(
        los_outcome_data, report_year, report_month, filter_key="ReportDate"
    )
    los_nonoutcome_data = truncate_report_to_data_month(
        los_nonoutcome_data, report_year, report_month, "ReportDate"
    )

    # --- Define file paths ---
    outcome_filename = "los_outcome_data.xlsx"
    outcome_path = f"{config.SERVER_PATH}/los_in_shelter/{outcome_filename}"

    non_outcome_filename = "los_nonoutcome_data.xlsx"
    non_outcome_path = f"{config.SERVER_PATH}/los_in_shelter/{non_outcome_filename}"

    dashboard_filename = "los_dashboard_template.xlsx"
    dashboard_path = f"{config.SERVER_PATH}/los_in_shelter/{dashboard_filename}"

    # --- Save normalized data ---
    normalize_excel_data_columns(los_outcome_data).to_excel(outcome_path, index=False)
    normalize_excel_data_columns(los_nonoutcome_data).to_excel(
        non_outcome_path, index=False
    )

    # --- Update dashboard ---
    update_dashboard(dashboard_path)

    # --- Archive copies with year/month appended ---
    make_archive_copy(
        report_year,
        report_month,
        base_path=f"{config.SERVER_PATH}/los_in_shelter",
        paths_to_copy=[outcome_path, non_outcome_path, dashboard_path],
    )

    return
