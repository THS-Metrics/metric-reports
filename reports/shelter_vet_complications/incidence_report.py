import pandas as pd
from datetime import datetime
import calendar
import numpy as np
from itertools import product
from database.ms_sql_connection import fetch_query
from utils.utils import (
    save_to_excel,
    save_single_to_excel,
    update_dashboard,
    combined_df,
    filter_last_12_months,
    make_archive_copy,
    truncate_report_to_data_month,
)
from environment.settings import config
from pandas.tseries.offsets import MonthEnd


def denominator_extraction(year: int, month: int) -> pd.DataFrame:
    """Extraction and Transformation script from SQL"""
    # Returns Last day of the month
    # Returns Last date of the month in the format YYYY-MM-DD
    last_date = datetime.strptime(
        f"{year}-{month:02}-{calendar.monthrange(year, month)[1]}", "%Y-%m-%d"
    )
    # Reference date
    reference_date = f"{year}-{month:02}-01"
    query = f"""
        SELECT
            a.AnimalID,
            a.Name,
            CASE
                WHEN rs.Species = 'Cat' THEN 'Cat'
                WHEN rs.Species = 'Dog' THEN 'Dog'
                ELSE 'Special Species'
            END AS Species,
            CASE
                WHEN a.Sex = 'U' AND et.Medication = 'Ovariohysterectomy' THEN 'F'
                WHEN a.Sex = 'U' AND et.Medication IN (
                    'Orchidectomy',
                    'Orchidectomy, cryptorchid',
                    'Orchidectomy Intra-Ab Crytorch',
                    'Orchidectomy Inguinal'
                ) THEN 'M'
                WHEN a.Sex IN ('F', 'M') THEN a.Sex
                ELSE a.Sex
            END AS Sex,
            ad.DateOfBirth,
            et.ExamTreatmentID AS UniqueSurgeryID,
            et.ExamID AS SurgeryID,
            et.Medication AS SurgeryType,
            CASE
                WHEN et.Medication IN (
                    'Dental Dehiscence Repair',
                    'Dental Extraction',
                    'Dental COHAT (Lv 1-3)',
                    'Dental COHAT (Lv 4-5)',
                    'Dental Extraction, Difficult',
                    'COHAT'
                ) THEN 'Dental'
                WHEN et.Medication IN (
                    'Orchidectomy',
                    'Orchidectomy, cryptorchid',
                    'Orchidectomy Intra-Ab Crytorch',
                    'Orchidectomy Inguinal'
                ) THEN 'Neuter'
                WHEN et.Medication = 'Ovariohysterectomy' THEN 'Spay'
                ELSE 'Other'
            END AS SurgeryCategory,
            et.StatusDateTime AS SurgeryDate,
            ROW_NUMBER() OVER (
                PARTITION BY a.AnimalID, et.Medication
                ORDER BY et.StatusDateTime
            ) AS Rank,
            p1.NameFirst + N' ' + p1.NameLast AS SurgeonName,
            p2.NameFirst + N' ' + p2.NameLast AS AssistantName,
            CASE
                WHEN s.SiteName IN (
                    'Toronto Humane Society',
                    'Toronto Humane Society Adoption Centre'
                ) THEN 'THS'
                WHEN s.SiteName IN (
                    'Toronto Humane Society Public Veterinary Services',
                    'Toronto Humane Society Spay Neuter Services',
                    'Toronto Humane Society Spay Neuter Services - (HSDR)'
                ) THEN 'PVS'
            END AS Site,
            s.SiteName,
            ras.Stage,
            rl.Location,
            hl.LastUpdated AS LocationDate
        FROM dbo.ExamTreatment et
        JOIN dbo.Animal a
            ON et.AnimalID = a.AnimalID
        JOIN HistoryStatus hs ON hs.AnimalID = a.AnimalID
        JOIN refAnimalStage ras ON ras.StageID = hs.StageID
        JOIN dbo.refSpecies rs
            ON a.SpeciesID = rs.SpeciesID
        JOIN dbo.AnimalDetails ad
            ON a.AnimalID = ad.AnimalID
        JOIN dbo.Site s
            ON et.SiteID = s.SiteID
        LEFT JOIN dbo.HistoryLocation hl
            ON a.AnimalID = hl.AnimalID
        LEFT JOIN dbo.refLocations rl
            ON hl.LocationID = rl.LocationID
        LEFT JOIN dbo.Person p1
            ON et.PerformedBy = p1.PersonID
        LEFT JOIN dbo.Person p2
            ON et.AssistantID = p2.PersonID
        WHERE
            et.Medication IN (
                'Urethrostomy, Perineal',
                'Amputation, Digit',
                'Amputation, Tail',
                'Orchidectomy Intra-Ab Crytorch',
                'Orchidectomy Inguinal',
                'Amputation, Hind Leg',
                'Laceration (minor)',
                'Rectal Prolapse Repair',
                'Amputation',
                'Biopsy',
                'Cherry eye repair',
                'Cruciate Ligament Repair',
                'Cystotomy',
                'Dental Dehiscence Repair',
                'Dental Extraction',
                'Dental COHAT (Lv 1-3)',
                'Dental COHAT (Lv 4-5)',
                'Dental Extraction, Difficult',
                'COHAT',
                'Ectropion correction',
                'Entropion Correction',
                'Enucleation',
                'Esophagostomy tube placement',
                'Exploratory Laparotomy',
                'Femoral head ostectomy',
                'Fracture repair',
                'Hernia repair',
                'Incision repair',
                'Mass Removal',
                'Orchidectomy',
                'Orchidectomy, cryptorchid',
                'Ovariohysterectomy',
                'Patella Luxation Surgery',
                'Total ear canal ablation',
                'Total hip replacement',
                'TPLO'
            )
            AND et.StatusDateTime BETWEEN '{reference_date}' AND '{last_date}'
        """
    df1 = fetch_query(query)
    # denominator_refined
    print(df1.shape)
    # Remove Off Site Animals
    df1 = df1[df1["Location"] != "Off Site Clinic"]
    # Sort animals, keep first record and remove duplicates
    df1 = df1.sort_values(
        ["AnimalID", "SurgeryDate", "UniqueSurgeryID", "SurgeryType", "LocationDate"]
    ).reset_index(drop=True)
    df1 = df1.drop_duplicates(subset=["AnimalID", "UniqueSurgeryID"])
    # Remove Duplicate Same Surgeries that happened sameday
    df1["SurgeryDate"] = pd.to_datetime(df1["SurgeryDate"]).dt.date
    df1 = df1.drop_duplicates(subset=["AnimalID", "SurgeryType", "SurgeryDate"])
    multiple = (
        df1.groupby(["AnimalID", "SurgeryDate", "SurgeryID"])
        .agg({"UniqueSurgeryID": np.size})
        .rename(columns={"UniqueSurgeryID": "num_surg_mapping"})
        .reset_index()
    )
    multiple = multiple[multiple["num_surg_mapping"] > 1]
    denominator_refined = pd.merge(
        df1,
        multiple,
        on=["AnimalID", "SurgeryDate", "SurgeryID"],
        how="outer",
        indicator=True,
    )
    denominator_refined["SurgeryCategory_refined"] = np.where(
        denominator_refined["_merge"] == "both",
        "Multiple",
        denominator_refined["SurgeryCategory"],
    )
    denominator_refined["num_surg_mapping"] = np.where(
        denominator_refined["num_surg_mapping"].isnull(),
        1,
        denominator_refined["num_surg_mapping"],
    )
    denominator_refined[denominator_refined["SurgeryCategory_refined"] == "Multiple"]
    del denominator_refined["_merge"]
    return denominator_refined


def numerator_extraction(year: int, month: int) -> pd.DataFrame:
    """Extraction and Transformation script from SQL"""
    # Returns Last day of the month
    # Returns Last date of the month in the format YYYY-MM-DD
    last_date = datetime.strptime(
        f"{year}-{month:02}-{calendar.monthrange(year, month)[1]}", "%Y-%m-%d"
    )
    # Reference date
    reference_date = f"{year}-{month:02}-01"

    # numerator
    query = f"""
        SELECT TOP (100) PERCENT
        a.AnimalID,
        a.Name,
        CASE
            WHEN rs.Species = 'Cat' THEN 'Cat'
            WHEN rs.Species = 'Dog' THEN 'Dog'
            ELSE 'Special Species'
        END AS Species,
        CASE
            WHEN a.Sex = 'U' AND et.Medication = 'Ovariohysterectomy' THEN 'F'
            WHEN a.Sex = 'U' AND et.Medication = 'Orchidectomy' THEN 'M'
            WHEN a.Sex = 'F' THEN 'F'
            WHEN a.Sex = 'M' THEN 'M'
            ELSE a.Sex
        END AS Sex,
        ad.DateOfBirth,
        et.ExamID AS SurgeryID,
        et.Medication AS SurgeryType,
        CASE
                WHEN et.Medication IN (
                    'Dental Dehiscence Repair',
                    'Dental Extraction',
                    'Dental COHAT (Lv 1-3)',
                    'Dental COHAT (Lv 4-5)',
                    'Dental Extraction, Difficult',
                    'COHAT'
                ) THEN 'Dental'
                WHEN et.Medication IN (
                    'Orchidectomy',
                    'Orchidectomy, cryptorchid',
                    'Orchidectomy Intra-Ab Crytorch',
                    'Orchidectomy Inguinal'
                ) THEN 'Neuter'
                WHEN et.Medication = 'Ovariohysterectomy' THEN 'Spay'
                ELSE 'Other'
            END AS SurgeryCategory,
        et.StatusDateTime AS SurgeryDate,
        rc.Condition AS SxComp,
        ec.DateTimeDiagnosed AS CompDate,
        ROW_NUMBER() OVER (
            PARTITION BY a.AnimalID, et.Medication
            ORDER BY et.StatusDateTime ASC
        ) AS Rank,
        DATEDIFF(DAY, et.DateCreated, ec.DateCreated) AS DaysAfterSurgery,
        p1.NameFirst + N' ' + p1.NameLast AS SurgeonName,
        p2.NameFirst + N' ' + p2.NameLast AS AssistantName,
        CASE
            WHEN s.SiteName IN (
                'Toronto Humane Society',
                'Toronto Humane Society Adoption Centre'
            ) THEN 'THS'
            WHEN s.SiteName IN (
                'Toronto Humane Society Public Veterinary Services',
                'Toronto Humane Society Spay Neuter Services',
                'Toronto Humane Society Spay Neuter Services - (HSDR)'
            ) THEN 'PVS'
        END AS Site,
        ec.ExamConditionID AS UniqueCompID,
        ec.ConditionID AS CompTypeID,
        et.ExamTreatmentID AS UniqueSurgeryID,
        et.TreatmentID AS SurgeryTypeID,
        rl.Location,
        hl.LastUpdated AS LocationDate
    FROM dbo.ExamTreatment et
    INNER JOIN dbo.Animal a
        ON et.AnimalID = a.AnimalID
    INNER JOIN dbo.AnimalDetails ad
        ON a.AnimalID = ad.AnimalID
    INNER JOIN dbo.refSpecies rs
        ON a.SpeciesID = rs.SpeciesID
    INNER JOIN dbo.Site s
        ON et.SiteID = s.SiteID
    INNER JOIN dbo.ExamCondition ec
        ON a.AnimalID = ec.AnimalID
    INNER JOIN dbo.refCondition rc
        ON ec.ConditionID = rc.ConditionID
    LEFT JOIN dbo.HistoryLocation hl
        ON a.AnimalID = hl.AnimalID
    LEFT JOIN dbo.refLocations rl
        ON hl.LocationID = rl.LocationID
    LEFT JOIN dbo.Person p1
        ON et.PerformedBy = p1.PersonID
    LEFT JOIN dbo.Person p2
        ON et.AssistantID = p2.PersonID
    WHERE
        rc.Condition IN (
            'Incision complications',
            'Surgical complication',
            'Anesthetic complication',
            'Anesthetic arrest'
        )
        AND et.Medication IN (
            'Urethrostomy, Perineal',
            'Amputation, Digit',
            'Amputation, Tail',
            'Orchidectomy Intra-Ab Crytorch',
            'Orchidectomy Inguinal',
            'Amputation, Hind Leg',
            'Laceration (minor)',
            'Rectal Prolapse Repair',
            'Amputation',
            'Biopsy',
            'Cherry eye repair',
            'Cruciate Ligament Repair',
            'Cystotomy',
            'Dental Dehiscence Repair',
            'Dental Extraction',
            'Dental COHAT (Lv 1-3)',
            'Dental COHAT (Lv 4-5)',
            'Dental Extraction, Difficult',
            'COHAT',
            'Ectropion correction',
            'Entropion Correction',
            'Enucleation',
            'Esophagostomy tube placement',
            'Exploratory Laparotomy',
            'Femoral head ostectomy',
            'Fracture repair',
            'Hernia repair',
            'Incision repair',
            'Mass Removal',
            'Orchidectomy',
            'Orchidectomy, cryptorchid',
            'Ovariohysterectomy',
            'Patella Luxation Surgery',
            'Total ear canal ablation',
            'Total hip replacement',
            'TPLO'
        )
        AND DATEDIFF(DAY, et.DateCreated, ec.DateCreated) >= -2
        AND et.StatusDateTime BETWEEN '{reference_date}' AND '{last_date}'
        AND ec.DateTimeDiagnosed BETWEEN '{reference_date}' AND '{last_date}';

    """
    df2 = fetch_query(query)
    # Remove Off Site Animals
    df2 = df2[df2["Location"] != "Off Site Clinic"]
    # df2=df2[df2['SurgeryType']!='Dental Dehiscence Repair']
    # df2=df2[df2['SurgeryCategory']!='Dental']
    # Sort animals, keep first record and remove duplicates
    df2 = df2.sort_values(["AnimalID", "UniqueSurgeryID", "UniqueCompID"]).reset_index(
        drop=True
    )
    df2 = df2.drop_duplicates(subset=["AnimalID", "UniqueSurgeryID", "UniqueCompID"])
    # Remove Duplicates
    df2["SurgeryDate"] = pd.to_datetime(df2["SurgeryDate"]).dt.date
    df2 = df2.drop_duplicates(subset=["AnimalID", "SurgeryType", "SxComp"])
    df2 = df2.sort_values(
        ["AnimalID", "SurgeryType", "SxComp", "SurgeryDate", "CompDate"]
    )
    numerator = df2.drop(["CompTypeID", "SurgeryTypeID"], axis=1)
    # get the complication that is the min days after surgery
    numerator_refined = (
        numerator.groupby(["AnimalID", "SxComp"])
        .agg({"DaysAfterSurgery": pd.Series.min})
        .reset_index()
    )
    numerator_refined = pd.merge(
        numerator_refined,
        numerator,
        on=["AnimalID", "SxComp", "DaysAfterSurgery"],
        how="inner",
    )
    numerator_refined["SurgeryDate"] = pd.to_datetime(
        numerator_refined["SurgeryDate"]
    ).dt.date
    numerator_refined["CompDate"] = pd.to_datetime(
        numerator_refined["CompDate"]
    ).dt.date
    # create the multiple category for if there were multiple surgeries for a given complicaiton
    multiple = (
        numerator_refined.groupby(["AnimalID", "SxComp", "SurgeryDate"])
        .agg({"SurgeryID": np.size})
        .rename(columns={"SurgeryID": "num_surg_mapping"})
        .reset_index()
    )
    multiple = multiple[multiple["num_surg_mapping"] > 1]
    numerator_refined = pd.merge(
        numerator_refined,
        multiple,
        on=["AnimalID", "SxComp", "SurgeryDate"],
        how="outer",
        indicator=True,
    )
    numerator_refined["SurgeryCategory_refined"] = np.where(
        numerator_refined["_merge"] == "both",
        "Multiple",
        numerator_refined["SurgeryCategory"],
    )
    numerator_refined["num_surg_mapping"] = np.where(
        numerator_refined["num_surg_mapping"].isnull(),
        1,
        numerator_refined["num_surg_mapping"],
    )
    # Remove unwanted columns
    numerator_refined = numerator_refined.drop(
        ["num_surg_mapping", "Location", "LocationDate", "_merge"], axis=1
    )

    return numerator_refined


def create_single_incidence_sheet(
    numerator: pd.DataFrame, denominator: pd.DataFrame
) -> pd.DataFrame:
    """Saves dataframe to Server and autofits columns"""
    numerator = numerator[
        [
            "AnimalID",
            "Name",
            "Species",
            "Sex",
            "SurgeryType",
            "SurgeryCategory",
            "SurgeryDate",
            "SurgeryCategory_refined",
            "SurgeonName",
        ]
    ]
    denominator = denominator[
        [
            "AnimalID",
            "Name",
            "Species",
            "Sex",
            "SurgeryType",
            "SurgeryCategory",
            "SurgeryDate",
            "SurgeryCategory_refined",
            "SurgeonName",
        ]
    ]
    numerator["Type"] = "Comp"
    denominator["Type"] = "NoComp"
    merged_data = pd.concat([denominator, numerator], ignore_index=True)
    # Convert SurgeryDate to datetime if it's not already
    merged_data["SurgeryDate"] = pd.to_datetime(
        merged_data["SurgeryDate"], errors="coerce"
    )
    # Creating SurgeryMonth column: extracting Year and Month from SurgeryDate
    merged_data["SurgeryMonth"] = merged_data["SurgeryDate"].dt.to_period("M")
    merged_data["reference_date"] = (
        merged_data["SurgeryDate"] + MonthEnd(0)
    ).dt.normalize()
    # Remove duplicates, prioritizing 'Comp' rows over 'NoComp' for each 'AnimalID' and 'SurgeryMonth'
    merged_data = merged_data.sort_values(
        by=["AnimalID", "SurgeryMonth", "SurgeryCategory", "Type"],
        ascending=[True, True, True, False],
    )
    merged_data = merged_data.drop_duplicates(
        subset=["AnimalID", "SurgeryMonth", "SurgeryCategory"], keep="last"
    )
    return merged_data


def get_complication_rate_sheet(incidence_sheet: pd.DataFrame) -> pd.DataFrame:
    summary = (
        incidence_sheet.groupby(
            ["reference_date", "SurgeryCategory_refined", "SurgeonName"]
        )
        .agg(
            TotalSurgeries=("AnimalID", "count"),
            Complications=("Type", lambda x: (x == "Comp").sum()),
        )
        .reset_index()
    )

    summary["ComplicationRate"] = (
        summary["Complications"] / summary["TotalSurgeries"] * 100
    ).round(2)

    summary = summary.sort_values(
        ["reference_date", "SurgeryCategory_refined", "SurgeonName"]
    )

    return summary


def process_bi_data(df):
    """
    Filters and processes surgery data to compute the percentage distribution of 'Type'
    for spay and neuter surgeries, grouped by SurgeryCategory_refined, SurgeryMonth, and SpeciesGroup.
    Missing months are filled with 0 percentages.

    Parameters:
        df (pd.DataFrame): Input DataFrame with required columns.

    Returns:
        pd.DataFrame: Combined DataFrame with all group combinations and 0s for missing ones.
    """
    # Select relevant columns
    df = df[["Species", "SurgeryCategory_refined", "SurgeryMonth", "Type"]]

    # Keep only Spay and Neuter surgeries
    df = df[df["SurgeryCategory_refined"].isin(["Spay", "Neuter"])]

    # Create species group
    df["SpeciesGroup"] = np.where(
        df["Species"] == "Special Species", "special", "cat_dog"
    )

    # Ensure SurgeryMonth is a string in YYYY-MM format
    # df["SurgeryMonth"] = pd.to_datetime(df["SurgeryMonth"]).dt.strftime('%Y-%m')

    # Get all unique values needed for combinations
    all_months = sorted(df["SurgeryMonth"].unique())
    all_groups = df["SpeciesGroup"].unique()
    all_categories = df["SurgeryCategory_refined"].unique()
    all_types = df["Type"].unique()

    # Create full cartesian product
    full_index = pd.DataFrame(
        list(product(all_groups, all_categories, all_months, all_types)),
        columns=["SpeciesGroup", "SurgeryCategory_refined", "SurgeryMonth", "Type"],
    )

    # Compute percentages
    grouped = (
        df.groupby(["SpeciesGroup", "SurgeryCategory_refined", "SurgeryMonth"])["Type"]
        .value_counts(normalize=True)
        .rename("Percentage")
        .mul(100)
        .reset_index()
    )

    # Merge with full index and fill missing values with 0
    final_df = (
        full_index.merge(
            grouped,
            on=["SpeciesGroup", "SurgeryCategory_refined", "SurgeryMonth", "Type"],
            how="left",
        )
        .fillna({"Percentage": 0})
        .sort_values(
            ["SpeciesGroup", "SurgeryCategory_refined", "SurgeryMonth", "Type"]
        )
        .reset_index(drop=True)
    )
    # Round percentages to 1 decimal place
    final_df["Percentage"] = final_df["Percentage"].round(1)

    return final_df


def run_incidence_report(report_year: int, report_month: int, run_bi_data: bool = True):
    """Generate incidence report and archive files by year/month"""

    base_path = f"{config.SERVER_PATH}/sxcomp"
    bi_base_path = f"{config.SERVER_PATH}/power_bi"

    # --- Generate numerator and denominator data ---
    numerator = combined_df(
        numerator_extraction, report_year, report_year, report_month
    )
    denominator = combined_df(
        denominator_extraction, report_year, report_year, report_month
    )

    numerator = truncate_report_to_data_month(
        numerator, report_year, report_month, filter_key="SurgeryDate"
    )
    denominator = truncate_report_to_data_month(
        denominator, report_year, report_month, filter_key="SurgeryDate"
    )
    one_year_incidence_sheet = create_single_incidence_sheet(numerator, denominator)
    comp_rate_data = get_complication_rate_sheet(one_year_incidence_sheet)

    # --- Define file paths ---
    files = {
        "report": f"{base_path}/incidence_complication.xlsx",
        "bi_report": f"{bi_base_path}/sx_comp_bi_report.xlsx",
        "dashboard": f"{base_path}/surgical_comp_dashboard_template.xlsx",
        "dashboard_data": f"{base_path}/incidence_complication_data.xlsx",
        "complication_rate_data": f"{base_path}/complication_rate_data.xlsx",
    }
    # --- Save main report ---
    save_to_excel(numerator, denominator, files["report"])
    save_single_to_excel(
        data=one_year_incidence_sheet,
        sheet_name="Denominator",
        path=files["dashboard_data"],
    )
    save_single_to_excel(
        data=comp_rate_data,
        sheet_name="ComplicationRates",
        path=files["complication_rate_data"],
    )

    # --- Optional BI dataset ---
    if run_bi_data:
        # --- Create dashboard data ---
        two_year_numerator = combined_df(
            numerator_extraction, report_year - 1, report_year
        )
        two_year_denominator = combined_df(
            denominator_extraction, report_year - 1, report_year
        )
        two_year_incidence_sheet = create_single_incidence_sheet(
            two_year_numerator, two_year_denominator
        )
        bi_data = filter_last_12_months(
            two_year_incidence_sheet, report_year, report_month, "SurgeryDate"
        )
        bi_data = process_bi_data(bi_data)
        bi_data.to_excel(files["bi_report"], index=False)

    # --- Update dashboard ---
    update_dashboard(files["dashboard"])

    # --- Archive copies ---
    make_archive_copy(
        report_year,
        report_month,
        base_path=base_path,
        paths_to_copy=[
            files["report"],
            files["dashboard"],
            files["dashboard_data"],
            files["complication_rate_data"],
        ],
    )

    print(
        f"Incidence report generation for {report_year}-{report_month:02d} completed."
    )
