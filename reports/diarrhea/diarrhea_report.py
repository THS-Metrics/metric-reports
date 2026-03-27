import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from database.ms_sql_connection import fetch_query
from utils.utils import save_to_excel, update_dashboard, combined_df, filter_last_12_months, make_archive_copy, truncate_report_to_data_month
from environment.settings import config
import calendar


def numerator(year, month): 
    """Reads query and performs calculation based on year 
    and month values"""
    #Constructs date value for first of every month
    reference_date=f'{year}-{month:02}-01'
    print(f"num ref date: {reference_date}")
    query=f"""WITH numerator
    AS (SELECT DISTINCT
      Animal.AnimalID,
      Animal.Name,
      refSpecies.Species,
      AnimalDetails.DateOfBirth,
      txnVisit.IntakeType,
      txnVisit.IntakeSubType,
      txnVisit.tin_DateCreated AS IntakeDate,
      refCondition.Condition,
      ExamCondition.ExamID,
      ExamCondition.DateCreated AS ExamDate,
      DATEDIFF(DAY, txnVisit.tin_DateCreated, ExamCondition.DateCreated) AS DaysAfterIntake
    FROM Animal
    INNER JOIN refSpecies
      ON Animal.SpeciesID = refSpecies.SpeciesID
    INNER JOIN AnimalDetails
      ON Animal.AnimalID = AnimalDetails.AnimalID
    INNER JOIN ExamCondition
    INNER JOIN txnVisit
      ON ExamCondition.DateCreated > txnVisit.tin_DateCreated
      ON Animal.AnimalID = txnVisit.AnimalID
      AND Animal.AnimalID = ExamCondition.AnimalID
    INNER JOIN refCondition
      ON ExamCondition.ConditionID = refCondition.ConditionID
    WHERE (refSpecies.Species IN ('Cat', 'Dog'))
    AND (txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND (refCondition.Condition IN ('Diarrhea', 'Diarrhea, acute, nonspecific',
    'Diarrhea and vomiting, acute, nonspecific'))
    AND DATEDIFF(DAY, txnVisit.tin_DateCreated, ExamCondition.DateCreated) BETWEEN 2 AND 365
    --Checks for examdates in current month only
    AND ExamCondition.DateCreated >=  '{reference_date}' and 
    ExamCondition.DateCreated <= EOMONTH('{reference_date}')
    )
    SELECT
      numerator.animalid,
      name,
      species,
      dateofbirth,
      MAX(intaketype) as Intaketype,
      condition,
      MAX(intakedate) AS intakedate,
      MIN(examdate) AS examdate,
      CAST('{reference_date}' AS date) AS Referencedate,
      CASE
        WHEN DATEDIFF(WEEK, numerator.dateofbirth, '{reference_date}') >= 20 THEN 'Adult'
        WHEN DATEDIFF(WEEK, numerator.dateofbirth, '{reference_date}') < 20 AND
          numerator.species = 'Cat' THEN 'Kitten'
        ELSE 'Puppy'
      END AS Agegroup
    FROM numerator
    WHERE DateOfBirth IS NOT NULL 
    

    GROUP BY numerator.animalid,
             name,
             species,
             condition,
             dateofbirth"""
    
    return fetch_query(query)

def denominator(year, month): 
    """Reads query and performs calculation based on year 
    and monthvalues"""
    #Constructs date value for first of every month
    reference_date=f'{year}-{month:02}-01'
    print(f"denom ref date: {reference_date}")
    query= f"""

    /*The Denominator dataset compiles information on all pets present in the shelter at the beginning of the month, 
    as well as pets that arrived during that month. 
    It merges data from both inventory records and intake records using a UNION operation.*/

    /*Beginning of CTEs for inventory table cleaning */
    --Inventory table contains history records of all animals present in the shelter.
    WITH inventory_table
    AS (SELECT DISTINCT
      Animal.AnimalID,
      Animal.Name,
      refSpecies.Species,
      AnimalDetails.DateOfBirth,
      refAnimalStage.Stage,
      HistoryStatus.LastUpdated AS StageDate,
      txnVisit.IntakeType,
      txnVisit.IntakeSubType,
      txnVisit.tin_DateCreated AS intakedate,
      HistoryStatus.Status
    FROM HistoryStatus
    INNER JOIN Animal
      ON HistoryStatus.AnimalID = Animal.AnimalID
    INNER JOIN refSpecies
      ON Animal.SpeciesID = refSpecies.SpeciesID
    INNER JOIN AnimalDetails
      ON Animal.AnimalID = AnimalDetails.AnimalID
    LEFT OUTER JOIN txnVisit
      ON txnVisit.AnimalID = HistoryStatus.AnimalID
      AND txnVisit.InPrimaryKey = HistoryStatus.OperationPrimaryID
    LEFT OUTER JOIN refAnimalStage
      ON HistoryStatus.StageID = refAnimalStage.StageID
    LEFT OUTER JOIN Stray
      ON txnVisit.IntakeSubTypeID = Stray.IntakeSubTypeID
      AND txnVisit.AnimalID = Stray.AnimalID
    LEFT OUTER JOIN TransferIn
      ON txnVisit.IntakeSubTypeID = TransferIn.IntakeSubTypeID
      AND txnVisit.AnimalID = TransferIn.AnimalID
    LEFT OUTER JOIN OwnerSurrender
      ON txnVisit.IntakeSubTypeID = OwnerSurrender.IntakeSubTypeID
      AND txnVisit.AnimalID = OwnerSurrender.AnimalID
    LEFT OUTER JOIN [Return]
      ON txnVisit.IntakeSubTypeID = [Return].IntakeSubTypeID
      AND txnVisit.AnimalID = [Return].AnimalID
    WHERE (refSpecies.Species IN ('Cat', 'Dog'))
    AND (refAnimalStage.Stage IN (N'Released', N'Pre-Euthanasia', N'Foster Program',
    N'Evaluate', N'Stray Holding - Feline',
    N'Stray Holding - Canine', N'Pre-Intake',
    N'Surgery Needed', N'Pending Behavior Assessment', N'Bite Quarantine', N'Medical Observation',
    N'Foster Needed', N'Medical Treatment', N'Behavior Observation'))
    AND (txnVisit.IntakeType IN ('TransferIn',
    'OwnerSurrender', '[Return]', 'Stray')
    OR txnVisit.IntakeType IS NULL)
    --Fetches data from one year ago.
    AND (txnVisit.tin_DateCreated >= DATEADD(YEAR, -1, '{reference_date}')
    OR txnVisit.tin_DateCreated IS NULL)
    AND (HistoryStatus.LastUpdated >= DATEADD(YEAR, -1, '{reference_date}'))),

    --latest_stage_date is a CTE of most recent stage dates for each pet between a year back and current report month
    latest_stagedate
    AS (SELECT
      animalid,
      name,
      MAX(stagedate) AS stagedate
    FROM inventory_table
    WHERE stagedate > DATEADD(YEAR, -1, '{reference_date}')
    AND StageDate <= '{reference_date}'
    GROUP BY animalid,
            name),

    --Past_intakes is a CTE of records in inventory table where intakedate is less than current month date
    Past_Intakes
    AS (SELECT
      animalid,
      MAX(intakedate) AS intakedate
    FROM inventory_table
    WHERE intakedate IS NOT NULL
    AND intakedate < '{reference_date}'
    GROUP BY animalid),
    /*End of CTEs for inventory table cleaning */


    /* Beginning of CTEs for intake records data cleaning*/
    --Total_Intake table generates records of all animals that came in during the month.
    Total_Intake
    AS (SELECT DISTINCT
      txnVisit.tin_DateCreated AS IntakeDate,
      Animal.AnimalID,
      Animal.Name,
      refSpecies.Species,
      AnimalDetails.DateOfBirth,
      txnVisit.IntakeType,
      txnVisit.IntakeSubType,
      refOperationStatus.OperationStatus
    FROM refSpecies
    INNER JOIN Animal
    INNER JOIN txnVisit
      ON Animal.AnimalID = txnVisit.AnimalID
      ON Animal.SpeciesID = refSpecies.SpeciesID
    INNER JOIN AnimalDetails
      ON AnimalDetails.AnimalID = Animal.AnimalID
    LEFT OUTER JOIN IntakeStatusHistory
    INNER JOIN refOperationStatus
      ON IntakeStatusHistory.StatusID = refOperationStatus.OperationStatusID
      ON txnVisit.tin_DateCreated = IntakeStatusHistory.StatusDateTime
      AND txnVisit.InPrimaryKey = IntakeStatusHistory.OperationRecordID
    LEFT OUTER JOIN refCondition
    INNER JOIN ExamCondition
      ON refCondition.ConditionID = ExamCondition.ConditionID
      ON txnVisit.AnimalID = ExamCondition.AnimalID
    WHERE (refSpecies.Species IN ('Cat', 'Dog'))
    AND (txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND (refOperationStatus.OperationStatus = 'Completed')
    AND txnVisit.tin_DateCreated >= '{reference_date}'
    AND txnVisit.tin_DateCreated <= EOMONTH('{reference_date}')),

    /*Intake_exclusion generates records of all animals to be excluded from intake records
    Animals who came to the shelter and developed diarrhea within one day of coming into the shelter need to be excluded*/
    intake_exclusion
    AS (SELECT
      Animal.AnimalID
    FROM Animal
    INNER JOIN refSpecies
      ON Animal.SpeciesID = refSpecies.SpeciesID
    INNER JOIN AnimalDetails
      ON Animal.AnimalID = AnimalDetails.AnimalID
    INNER JOIN ExamCondition
    INNER JOIN txnVisit
      ON ExamCondition.DateCreated > txnVisit.tin_DateCreated
      ON Animal.AnimalID = txnVisit.AnimalID
      AND Animal.AnimalID = ExamCondition.AnimalID
    INNER JOIN refCondition
      ON ExamCondition.ConditionID = refCondition.ConditionID
    WHERE (refSpecies.Species IN ('Cat', 'Dog'))
    AND (txnVisit.IntakeType IN ('TransferIn', 'Stray', '[Return]', 'OwnerSurrender'))
    AND (refCondition.Condition IN ('Diarrhea', 'Diarrhea, acute, nonspecific',
    'Diarrhea and vomiting, acute, nonspecific'))
    AND DATEDIFF(DAY, txnVisit.tin_DateCreated, ExamCondition.DateCreated) BETWEEN 0 AND 1
    AND ExamCondition.DateCreated >= '{reference_date}'
    AND ExamCondition.DateCreated <= EOMONTH('{reference_date}'))
    /* End of CTEs for intake records data cleaning*/


    /*Beginning of data cleaning for inventory dataset*/

    --Obtains corresponding status and intakedate for most recent stagedate from latest_stagedate table
    --max(inventory_table.status) was used to obtain only one entry in situations where there was more than one status for same date.
    --Thus it assigns the maximum status(I=Inactive) of such occurences since such pets are to be ignored.
    --Min(inventory_table.stage) selects the first stage entry in cases where there are 2 stages for the same stagedate/status

    SELECT
      latest_stagedate.animalid,
      latest_stagedate.name,
      inventory_table.species,
      inventory_table.dateofbirth,
      latest_stagedate.stagedate,
      MAX(inventory_table.status) AS status,
      MIN(inventory_table.stage) AS stage,
      Past_Intakes.intakedate,
      CAST('{reference_date}' AS date) AS Referencedate,
      CASE
        WHEN DATEDIFF(WEEK, inventory_table.dateofbirth, '{reference_date}') >= 20 THEN 'Adult'
        WHEN DATEDIFF(WEEK, inventory_table.dateofbirth, '{reference_date}') < 20 AND
          inventory_table.species = 'Cat' THEN 'Kitten'
        ELSE 'Puppy'
      END AS Agegroup
    FROM latest_stagedate
    INNER JOIN inventory_table
      ON latest_stagedate.stagedate = inventory_table.stagedate
      AND latest_stagedate.animalid = inventory_table.animalid
    INNER JOIN Past_Intakes
      ON latest_stagedate.animalid = Past_Intakes.animalid
    WHERE inventory_table.status = 'A'
    AND dateofbirth IS NOT NULL

    GROUP BY latest_stagedate.animalid,
            inventory_table.species,
            latest_stagedate.stagedate,
            Past_Intakes.intakedate,
            latest_stagedate.name,
            inventory_table.dateofbirth
    /*End of data cleaning for inventory dataset*/

    UNION ALL

    /*Beginning of data cleaning for intake dataset.
    Generates dataset of all pets who came into the shelter during the month and excludes pets
    who generated diarrhea within one day of coming into the shelter.*/

    SELECT
      total_intake.animalid,
      total_intake.name,
      total_intake.species,
      total_intake.dateofbirth,
      CAST('{reference_date}' AS date) AS Stagedate,
      'A' AS status,
      'Intake' AS stage,
      MAX(Total_Intake.intakedate),
      CAST('{reference_date}' AS date) AS Referencedate,
      CASE
        WHEN DATEDIFF(WEEK, total_intake.dateofbirth, '{reference_date}') >= 20 THEN 'Adult'
        WHEN DATEDIFF(WEEK, total_intake.dateofbirth, '{reference_date}') < 20 AND
          total_intake.species = 'Cat' THEN 'Kitten'
        ELSE 'Puppy'
      END AS Agegroup
    FROM Total_Intake
    --Excludes pets who developed diarrhea within one day of coming into shelter
    WHERE NOT EXISTS (SELECT
      *
    FROM intake_exclusion
    WHERE intake_exclusion.AnimalID = Total_Intake.animalid)

    AND dateofbirth IS NOT NULL
    GROUP BY total_intake.animalid,
            total_intake.name,
            total_intake.species,
            total_intake.dateofbirth
    ORDER BY animalid
    """
    return fetch_query(query)


def parse_combined_data(function,report_year) -> pd.DataFrame:
    """Combines the dataframes for each year and months into a single dataframe"""
    df=combined_df(function, report_year, report_year)
    try:
      df[["dateofbirth", "intakedate", "Referencedate", "examdate"]] = df[["dateofbirth", "intakedate", "Referencedate", "examdate"]].apply(
          lambda x: pd.to_datetime(x).dt.date)
    except:
      df[["dateofbirth", "intakedate", "Referencedate", "stagedate"]] = df[["dateofbirth", "intakedate", "Referencedate", "stagedate"]].apply(
        lambda x: pd.to_datetime(x).dt.date)
    return df

def diarrhea_chart_data(*,numerator, denominator, path) -> None:
  '''Creates harmonized records of numerators and denominators.
  This data is responsible for the dashboard building.'''
  #Creates an Outcome column specifying all numerator as infected.

  numerator["Outcome"]="Infected"
  #Assigns a denominator string to all intake types of denominator.
  #Since we need to pull intaketypes from numerator for dashboard.
  #Creates an Outcome column specifying all numerator as Healthy.
  denominator["Outcome"]="Healthy" 
  denominator["Intaketype"]="Denominator"	
  chart_data=pd.concat(
    [denominator[['animalid', 'species', 'Agegroup','Outcome','Intaketype', 'Referencedate']], 
    numerator[['animalid', 'species', 'Agegroup','Outcome','Intaketype', 'Referencedate']]],
    ignore_index=True
    )
  #We will assign value of 0 to virtual data values used to account for months with no incidence data
  chart_data['sum']=1
  # Creating a Referencedate range with the missing months
  min_Referencedate = chart_data['Referencedate'].min()
  max_Referencedate = chart_data['Referencedate'].max()
  Referencedate_range = pd.date_range(min_Referencedate, max_Referencedate, freq='MS').date

  # Generating the missing rows for dogs
  missing_rows = []
  for agegroup in ['Adult', 'Puppy']:
    existing_Referencedates = chart_data[(chart_data['Outcome'] == 'Infected') & (chart_data['species'] == 'Dog') & (chart_data['Agegroup'] == agegroup)]['Referencedate']
    missing_Referencedates = set(Referencedate_range) - set(existing_Referencedates)
    for Referencedate in missing_Referencedates:
        missing_rows.append({'Referencedate': Referencedate, 'Agegroup': agegroup, 'species': 'Dog', 'Outcome': 'Infected', 'sum':0})
   # Generating the missing rows for cats
  for agegroup in ['Adult', 'Kitten']:
    existing_Referencedates = chart_data[(chart_data['Outcome'] == 'Infected') & (chart_data['species'] == 'Cat') & (chart_data['Agegroup'] == agegroup)]['Referencedate']
    missing_Referencedates = set(Referencedate_range) - set(existing_Referencedates)
    for Referencedate in missing_Referencedates:
        missing_rows.append({'Referencedate': Referencedate, 'Agegroup': agegroup, 'species': 'Cat', 'Outcome': 'Infected', 'sum':0})
  
  # Convert missing rows to DataFrame before concatenating
  missing_df = pd.DataFrame(missing_rows)
  # Appending the missing rows to the DataFrame
  chart_data=pd.concat([chart_data, missing_df], ignore_index=True)
  chart_data.to_excel(path, index=False)
  return chart_data


def process_incidence_bi_data(df, report_year, report_month):
    df['Referencedate'] = pd.to_datetime(df['Referencedate'])
    df['Month'] = df['Referencedate'].dt.to_period('M')
    grouped = df.groupby(['species', 'Month', 'Agegroup', 'Outcome'])['sum'].sum().reset_index()
    # Get current month as Period (e.g., '2025-05')
    # Use report year/month instead of today
    current_month = pd.Period(f"{report_year}-{str(report_month).zfill(2)}", freq='M')
    # Remove records from the current month
    df = df[df['Month'] != current_month]
    
    pivot_df = grouped.pivot_table(
        index=['species', 'Month', 'Agegroup'],
        columns='Outcome',
        values='sum',
        fill_value=0
    ).reset_index()
    
    # Ensure required columns exist
    for col in ['Infected', 'Healthy']:
        if col not in pivot_df.columns:
            pivot_df[col] = 0
    
    pivot_df['infection_percent'] = pivot_df['Infected'] / pivot_df['Healthy']
    pivot_df['infection_percent'] = (pivot_df['infection_percent'] * 100).round(2)
    
    return pivot_df[['species', 'Month', 'Agegroup', 'infection_percent']]



def run_diarrhea_report(report_year: int, report_month: int, run_bi_data: bool = True):
    """Generate diarrhea report and archive files by year/month"""

    base_path = f"{config.SERVER_PATH}/diarrhea"
    bi_base_path = f"{config.SERVER_PATH}/power_bi"

    def load_and_truncate(data_source, year):
        df = parse_combined_data(data_source, year)
        return truncate_report_to_data_month(df, report_year, report_month, filter_key="Referencedate")

    
    # --- Current year data ---
    df_denom = load_and_truncate(denominator, report_year)
    df_num = load_and_truncate(numerator, report_year)
    
    # --- File paths ---
    files = {
        "bi_report": f"{bi_base_path}/diarrhea_infection_percent_bi_data.xlsx",
        "report": f"{base_path}/diarrhea_report_raw_data.xlsx",
        "chart": f"{base_path}/diarrhea_chart_data.xlsx",
        "dashboard": f"{base_path}/diarrhea_dashboard_template.xlsx",
    }

    # --- Generate Power BI dataset ---
    if run_bi_data:
        # --- Previous year data ---
        df_denom_prev = parse_combined_data(denominator, report_year - 1)
        df_num_prev = parse_combined_data(numerator, report_year - 1)

        two_year_denom = truncate_report_to_data_month(
            pd.concat([df_denom_prev, df_denom], ignore_index=True),
            report_year, report_month, filter_key="Referencedate"
        )

        two_year_num = truncate_report_to_data_month(
            pd.concat([df_num_prev, df_num], ignore_index=True),
            report_year, report_month, filter_key="Referencedate"
        )

        bi_data = diarrhea_chart_data(
            path=files["bi_report"],
            numerator=two_year_num,
            denominator=two_year_denom
        )

        bi_data = filter_last_12_months(bi_data, report_year, report_month, "Referencedate")
        bi_data = process_incidence_bi_data(bi_data, report_year, report_month)

        bi_data.to_excel(files["bi_report"], index=False)

    # --- Main report outputs ---
    save_to_excel(path=files["report"], numerator=df_num, denominator=df_denom)

    diarrhea_chart_data(
        path=files["chart"],
        numerator=df_num,
        denominator=df_denom
    )

    update_dashboard(files["dashboard"])

    # --- Archive copies ---
    make_archive_copy(
        report_year,
        report_month,
        base_path=base_path,
        paths_to_copy=[files["report"], files["chart"], files["dashboard"]],
    )

    print(f"Diarrhea report generation for {report_year}-{report_month:02d} completed.")
