from reports.diarrhea.diarrhea_report import run_diarrhea_report
from reports.kitten_mortality.kitten_report import run_kitten_report
from reports.parvovirus.parvo_report import run_parvo_report
from reports.shelter_vet_complications.incidence_report import run_incidence_report
from reports.dental.dental_report import run_dental_report
from reports.ringworm.ringworm_report import run_ringworm_report
from reports.uri.uri_report import run_uri_report
from reports.surgery_wait_time.sx_wait_time import run_sx_wait_time_report
from reports.delayed_euthanasia.delayed_euthanasia import run_euthanasia_report
from reports.los_shelter.los_shelter_report import run_los_report
#from reports.public_vet_complications.ezyvet import get_ezyvet_report
from datetime import datetime

def run_all():
    report_year= datetime.now().year
    #scheduled to run report for previous month
    report_month=datetime.now().month -1
    run_diarrhea_report(report_year,report_month)
    run_kitten_report(report_year,report_month)
    run_parvo_report(report_year,report_month)
    run_dental_report(report_year,report_month)
    run_incidence_report(report_year,report_month)
    run_ringworm_report(report_year,report_month)
    run_uri_report(report_year,report_month)
    run_los_report(report_year,report_month)
    run_sx_wait_time_report(report_year,report_month)
    run_euthanasia_report(report_year, report_month)
    return 


if __name__ == "__main__":
    #run_all()
    #run_kitten_report(2026, 1)
    #run_kitten_report(2026,2)
    run_los_report(2026, 1)