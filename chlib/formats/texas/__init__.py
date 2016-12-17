__author__ = 'aub3'
import schema,gzip
from collections import defaultdict
from ...entity import visit
from ...entity import penums_pb2 as enums
import logging
import random
random.seed()


def parse(line):
    p = visit.Patient()
    p.raw = line
    v = p.visits.add()
    transfrom_line(line,v)
    visit.index_procedures(v)
    return p


def transfrom_line(line,v):
    tabbed_entries = line.split("\t")
    quarter = tabbed_entries[0]
    year = tabbed_entries[1]
    line = tabbed_entries[2:]
    v.key = schema.get_element('RECORD_ID',line)
    v.dataset = 'TX'
    v.state = 'TX'
    v.patient_key = schema.get_element('RECORD_ID',line)
    v.facility = schema.get_element('THCIC_ID',line)
    v.charge = float(schema.get_element('TOTAL_CHARGES',line))
    v.vtype = enums.IP
    los = schema.get_element('LENGTH_OF_STAY',line)
    if los.strip():
        v.los = int(los)
    else:
        v.los = -1
    v.dnr = enums.DNR_UNAVAILABLE
    v.zip = enums.Z_UNKNOWN
    v.year = int(year)
    v.day = -1
    v.month = -1
    v.quarter = int(quarter)
    sex_code = schema.get_element("SEX_CODE",line)
    if sex_code == 'M':
        v.sex = enums.MALE
    elif sex_code == 'F':
        v.sex = enums.FEMALE
    else:
        v.sex = enums.SEX_UNKNOWN
    add_source(v,line)
    add_disp(v,line)
    add_race(v,line)
    add_payer(v,line)
    add_age(v,line)
    add_codes(v,line)
    if v.year == 2006 or v.year == 2008 or v.year == 2009:
        v.drg = 'DG-1'


def add_source(v,line):
    line_source = schema.get_element('SOURCE_OF_ADMISSION',line)
    if line_source == '1' or line_source == '2' or line_source == '3':
        v.source = enums.S_ROUTINE
        # 1 Physician referral
        # 2 Clinic referral
        # 3 HMO referral
    elif line_source == '4':
        v.source = enums.S_HOSPITAL     # 4 Transfer from a hospital
    elif line_source == '5': # 5 Transfer from a skilled nursing facility,  6 Transfer from another health care facility
        v.source = enums.S_SNF
    elif line_source =='6': # 5 Transfer from a skilled nursing facility,  6 Transfer from another health care facility
        v.source = enums.S_OTHER
    elif line_source == '7': # 7 Emergency Room
        v.source = enums.S_ED
    elif line_source == '8': # 8 Court/Law Enforcement
        v.source = enums.S_COURT
    elif line_source == '9': # 9 Information not available
        v.source = enums.S_UNKNOWN
    elif line_source == '0':
        v.source = enums.S_TEXAS_PSYCH     # 0 Transfer from psychiatric, substance abuse, rehab hospital
    elif line_source == 'A':
        v.source = enums.S_TEXAS_CRITICAL     # A Transfer from a critical access hospital
    elif line_source == 'D':
        v.source = enums.S_TEXAS_INTERNAL     # D Transfer from Hospital Inpatient in the Same Facility Resulting in a Separate Claim to the Payer, effective
    else:
        v.source = enums.S_UNKNOWN


def add_disp(v,line):
    """

    :param v:
    :param line:
    :return:

    """
    dispcode = schema.get_element("PAT_STATUS",line).strip()
    v.death = enums.ALIVE

    if dispcode == "01": #Discharged to home or self-care (routine discharge)
        v.disposition = enums.D_ROUTINE
    elif dispcode == "02": #Discharged to other short term general hospital
        v.disposition = enums.D_HOSPITAL
    elif dispcode == "03": #Discharged to skilled nursing facility
        v.disposition = enums.D_SNF
    elif dispcode == "04": #Discharged to intermediate care facility
        v.disposition = enums.D_ICF
    elif dispcode == "05": #Discharged to another type of health care facility not elsewhere listed
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "06": #Discharged to care of home health service
        v.disposition = enums.D_HOME
    elif dispcode == "07": #Left against medical advice
        v.disposition = enums.D_AMA
    elif dispcode == "08": #Discharged to care of Home IV provider
        v.disposition = enums.D_OTHER
    elif dispcode == "09": #Admitted as inpatient to this hospital
        v.disposition = enums.D_OTHER
    elif dispcode == "20": # Expired
        v.disposition = enums.D_DEATH
        v.death = enums.DEAD
    elif dispcode == "30": # Still patient
        v.disposition = enums.D_OTHER
    elif dispcode == "40": # Expired at home
        v.disposition = enums.D_DEATH
        v.death = enums.DEAD
    elif dispcode == "41": # Expired in a medical facility
        v.disposition = enums.D_DEATH
        v.death = enums.DEAD
    elif dispcode == "42": # Expired, place unknown
        v.disposition = enums.D_DEATH
        v.death = enums.DEAD
    elif dispcode == "43": # Discharged/transferred to federal health care facility
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "50": # discharged to hospice home
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "51": # Discharged to hospice medical
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "61": # Discharged/transferred within this institution to Medicare-approved swing bed
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "62": # Discharged/transferred to inpatient rehabilitation facility
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "63": # Discharged/transferred to Medicare-certified long term care hospital
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "64": # Discharged/transferred to Medicaid-certified nursing facility
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "65": # Discharged/transferred to psychiatric hospital or psychiatric distinct part of a hospital
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "66": # Discharged/transferred to Critical Access Hospital (CAH)
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "71": # Discharged/transferred to other outpatient service
        v.disposition = enums.D_UNKNOWN
    elif dispcode == "72": # Discharged/transferred to institution outpatient
        v.disposition = enums.D_UNKNOWN
    else:
        v.disposition = enums.D_UNKNOWN


def add_age(v,line):
    """
    Field 24: PAT_AGE
    Description: Code indicating age of patient in days or years on date of discharge.
    00 1-28 days 10 35-39 20 85-89
    01 29-365 days 11 40-44 21 90+
    02 1-4 years 12 45-49 HIV and drug/alcohol use patients:
    03 5-9 13 50-54 22 0-17
    04 10-14 14 55-59 23 18-44
    05 15-17 15 60-64 24 45-64
    06 18-19 16 65-69 25 65-74
    07 20-24 17 70-74 26 75+
    08 25-29 18 75-79 * Invalid
    09 30-34 19 80-84
    """
    age_code = schema.get_element("PAT_AGE",line)
    age_map = {
        "00":0,
        "01":0,
        "02":1,
        "03":5,
        "04":10,
        "05":15,
        "06":18,
        "07":20,
        "08":25,
        "09":30,
        "10":35,
        "11":40,
        "12":45,
        "13":50,
        "14":55,
        "15":60,
        "16":65,
        "17":70,
        "18":75,
        "19":80,
        "20":85,
        "21":90,
        "22":0,
        "23":18,
        "24":45,
        "25":65,
        "26":75,
    }
    if age_code in age_map:
        v.age = age_map[age_code]


def add_payer(v,line):
    payer_code = schema.get_element("FIRST_PAYMENT_SRC",line)
    if payer_code =='09':# Self Pay
        v.payer = enums.SELF
    elif payer_code =='HM':# Health Maintenance Organization
        v.payer = enums.PRIVATE
    elif payer_code =='10':# Central Certelification
        v.payer = enums.P_UNKNOWN
    elif payer_code =='LI':# Liability
        v.payer = enums.P_UNKNOWN
    elif payer_code =='11':# Other Non-federal Programs
        v.payer = enums.P_UNKNOWN
    elif payer_code =='LM':# Liability Medical
        v.payer = enums.P_UNKNOWN
    elif payer_code =='12':# Preferred Provider Organization (PPO)
        v.payer = enums.PRIVATE
    elif payer_code =='MA':# Medicare Part A
        v.payer = enums.MEDICARE
    elif payer_code =='13':# Point of Service (POS)
        v.payer = enums.PRIVATE
    elif payer_code =='MB':# Medicare Part B
        v.payer = enums.MEDICARE
    elif payer_code =='14':# Exclusive Provider Organization (EPO)
        v.payer = enums.PRIVATE
    elif payer_code =='MC':# Medicaid
        v.payer = enums.MEDICAID
    elif payer_code =='15':# Indemnity Insurance
        v.payer = enums.OTHER
    elif payer_code =='TV':# Title V
        v.payer = enums.OTHER
    elif payer_code =='16':# Health Maintenance Organization (HMO)
        v.payer = enums.PRIVATE
    elif payer_code =='OF':# Other Federal Program
        v.payer = enums.OTHER
    elif payer_code =='AM':# Automobile Medical
        v.payer = enums.OTHER
    elif payer_code =='VA':# Veteran Administration Plan
        v.payer = enums.OTHER
    elif payer_code =='BL':# Blue Cross/Blue Shield
        v.payer = enums.PRIVATE
    elif payer_code =='WC':# Workers Compensation Health Claim
        v.payer = enums.PRIVATE
    elif payer_code =='CH':# CHAMPUS
        v.payer = enums.OTHER
    elif payer_code =='ZZ':# Charity, Indigent or Unknown
        v.payer = enums.OTHER
    elif payer_code =='CI':# Commercial Insurance ** Codes 09 and ZZ, combined for 2004 & 2005
        v.payer = enums.OTHER
    elif payer_code =='DS':# Disability Insurance * Invalid
        v.payer = enums.OTHER
    else:
        v.payer = enums.P_UNKNOWN

def add_race(v,line):
    """
    1 American Indian/Eskimo/Aleut
    2 Asian or Pacific Islander
    3 Black
    4 White
    5 Other
    * Invalid
    """
    race_code = schema.get_element("RACE",line).strip()
    if race_code == '1':
        v.race = enums.NATIVE
    elif race_code == '2':
        v.race = enums.ASIAN
    elif race_code == '3':
        v.race = enums.BLACK
    elif race_code =='4':
        v.race = enums.WHITE
    elif race_code == '5':
        v.race = enums.R_OTHER
    else:
        v.race = enums.R_UNKNOWN


def add_codes(v,line):
    drg = schema.get_element("HCFA_DRG",line)
    if drg.strip():
        v.drg = 'DG'+str(int(drg))
    for k in filter(None,[schema.get_element('E_CODE_'+str(k),line).strip() for k in range(1,11)]):
        v.exs.append('E'+k)
    primary_dx = schema.get_element('PRINC_DIAG_CODE',line)
    if primary_dx:
        v.primary_diagnosis = 'D'+primary_dx
        v.dxs.append('D'+primary_dx)
    else:
        v.primary_diagnosis = ""
    for k in filter(None,[schema.get_element('OTH_DIAG_CODE_'+str(k),line).strip() for k in range(1,25)]):
        v.dxs.append('D'+k)
    if schema.get_element('PRINC_SURG_PROC_CODE',line).strip() != schema.get_element('PRINC_ICD9_CODE',line).strip():
        # print (schema.get_element('PRINC_SURG_PROC_CODE',line),schema.get_element('PRINC_ICD9_CODE',line),line)
        pass
    primary_pr = schema.get_element('PRINC_SURG_PROC_CODE',line).strip()
    primary_day = schema.get_element('PRINC_SURG_PROC_DAY',line)
    if primary_pr:
        v.primary_procedure.pcode = 'P'+primary_pr
        v.primary_procedure.ctype = enums.ICD
        if primary_day.strip():
            if primary_day[0] == '+':
                v.primary_procedure.pday = int(primary_day[1:])
            elif primary_day[0] == '-':
                v.primary_procedure.pday = -int(primary_day[1:])
        v.prs.add().CopyFrom(v.primary_procedure)
    for ind in range(1,25):
        ind = str(ind)
        pcode = schema.get_element('OTH_SURG_PROC_CODE_'+ind,line).strip()
        pday = schema.get_element('OTH_SURG_PROC_DAY_'+ind,line)
        if pcode != schema.get_element('OTH_ICD9_CODE_'+ind,line).strip():
            # print (ind,pcode,schema.get_element('OTH_ICD9_CODE_'+ind,line),line)
            pass
        if pcode:
            temp = v.prs.add()
            temp.pcode = 'P'+pcode
            temp.ctype = enums.ICD
            if pday.strip():
                if pday[0] == '+':
                    temp.pday = int(pday[1:])
                elif pday[0] == '-':
                    temp.pday = -int(pday[1:])
    # print v.primary_procedure
    # print '\nPRS entry:'.join([k.__str__() for k in v.prs])
    # print "\n"*3


def process_buffer_texas(line):
    p = parse(line)
    return p.visits[0].key.encode("utf-8"),p.SerializeToString()


def finalize_texas(res,db):
    wb = db.write_batch()
    for patient_key,pstr in res:
        wb.put(patient_key,pstr)
    wb.write()
