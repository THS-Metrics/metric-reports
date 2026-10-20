from datetime import datetime, timedelta
from reports.diarrhea.diarrhea_report import run_diarrhea_report
from reports.kitten_mortality.kitten_report import run_kitten_report
from reports.parvovirus.parvo_report import run_parvo_report
from reports.shelter_vet_complications.incidence_report import run_incidence_report
from reports.ringworm.ringworm_report import run_ringworm_report
from reports.uri.uri_report import run_uri_report
from reports.surgery_wait_time.sx_wait_time import run_sx_wait_time_report
from reports.delayed_euthanasia.delayed_euthanasia import run_euthanasia_report
from reports.los_shelter.los_shelter_report import run_los_report
from database.db_sync import update_db


def run_all(report_year: int, report_month: int, update_power_bi: bool):
    update_db()
    run_diarrhea_report(report_year, report_month, run_bi_data=update_power_bi)
    run_kitten_report(report_year, report_month, run_bi_data=update_power_bi)
    run_parvo_report(report_year, report_month)
    run_incidence_report(report_year, report_month, run_bi_data=update_power_bi)
    run_ringworm_report(report_year, report_month)
    run_uri_report(report_year, report_month, run_bi_data=update_power_bi)
    run_los_report(report_year, report_month)
    run_sx_wait_time_report(report_year, report_month, run_bi_data=update_power_bi)
    run_euthanasia_report(report_year, report_month)


if __name__ == "__main__":
    # Scheduled to run reports for the previous month
    # previous_month = datetime.now().replace(day=1) - timedelta(days=1)
    report_year = 2026
    report_month = 9
    update_power_bi = True
    run_all(
        report_year=report_year,
        report_month=report_month,
        update_power_bi=update_power_bi,
    )
