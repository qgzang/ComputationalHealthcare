import re,os,gzip,logging
from ...entity import enums,visit

FORMAT = {
    "2013":"""
NRD  2013 Core        1 AGE                  3   Num  Age in years at admission
NRD  2013 Core        2 AWEEKEND             2   Num  Admission day is a weekend
NRD  2013 Core        3 DIED                 2   Num  Died during hospitalization
NRD  2013 Core        4 DISCWT              11.7 Num  Weight to discharges in AHA universe
NRD  2013 Core        5 DISPUNIFORM          2   Num  Disposition of patient (uniform)
NRD  2013 Core        6 DMONTH               2   Num  Discharge month
NRD  2013 Core        7 DQTR                 2   Num  Discharge quarter
NRD  2013 Core        8 DRG                  3   Num  DRG in effect on discharge date
NRD  2013 Core        9 DRGVER               2   Num  DRG grouper version used on discharge date
NRD  2013 Core       10 DRG_NoPOA            3   Num  DRG in use on discharge date, calculated without POA
NRD  2013 Core       11 DX1                  5   Char Diagnosis 1
NRD  2013 Core       12 DX2                  5   Char Diagnosis 2
NRD  2013 Core       13 DX3                  5   Char Diagnosis 3
NRD  2013 Core       14 DX4                  5   Char Diagnosis 4
NRD  2013 Core       15 DX5                  5   Char Diagnosis 5
NRD  2013 Core       16 DX6                  5   Char Diagnosis 6
NRD  2013 Core       17 DX7                  5   Char Diagnosis 7
NRD  2013 Core       18 DX8                  5   Char Diagnosis 8
NRD  2013 Core       19 DX9                  5   Char Diagnosis 9
NRD  2013 Core       20 DX10                 5   Char Diagnosis 10
NRD  2013 Core       21 DX11                 5   Char Diagnosis 11
NRD  2013 Core       22 DX12                 5   Char Diagnosis 12
NRD  2013 Core       23 DX13                 5   Char Diagnosis 13
NRD  2013 Core       24 DX14                 5   Char Diagnosis 14
NRD  2013 Core       25 DX15                 5   Char Diagnosis 15
NRD  2013 Core       26 DX16                 5   Char Diagnosis 16
NRD  2013 Core       27 DX17                 5   Char Diagnosis 17
NRD  2013 Core       28 DX18                 5   Char Diagnosis 18
NRD  2013 Core       29 DX19                 5   Char Diagnosis 19
NRD  2013 Core       30 DX20                 5   Char Diagnosis 20
NRD  2013 Core       31 DX21                 5   Char Diagnosis 21
NRD  2013 Core       32 DX22                 5   Char Diagnosis 22
NRD  2013 Core       33 DX23                 5   Char Diagnosis 23
NRD  2013 Core       34 DX24                 5   Char Diagnosis 24
NRD  2013 Core       35 DX25                 5   Char Diagnosis 25
NRD  2013 Core       36 DXCCS1               3   Num  CCS: diagnosis 1
NRD  2013 Core       37 DXCCS2               3   Num  CCS: diagnosis 2
NRD  2013 Core       38 DXCCS3               3   Num  CCS: diagnosis 3
NRD  2013 Core       39 DXCCS4               3   Num  CCS: diagnosis 4
NRD  2013 Core       40 DXCCS5               3   Num  CCS: diagnosis 5
NRD  2013 Core       41 DXCCS6               3   Num  CCS: diagnosis 6
NRD  2013 Core       42 DXCCS7               3   Num  CCS: diagnosis 7
NRD  2013 Core       43 DXCCS8               3   Num  CCS: diagnosis 8
NRD  2013 Core       44 DXCCS9               3   Num  CCS: diagnosis 9
NRD  2013 Core       45 DXCCS10              3   Num  CCS: diagnosis 10
NRD  2013 Core       46 DXCCS11              3   Num  CCS: diagnosis 11
NRD  2013 Core       47 DXCCS12              3   Num  CCS: diagnosis 12
NRD  2013 Core       48 DXCCS13              3   Num  CCS: diagnosis 13
NRD  2013 Core       49 DXCCS14              3   Num  CCS: diagnosis 14
NRD  2013 Core       50 DXCCS15              3   Num  CCS: diagnosis 15
NRD  2013 Core       51 DXCCS16              3   Num  CCS: diagnosis 16
NRD  2013 Core       52 DXCCS17              3   Num  CCS: diagnosis 17
NRD  2013 Core       53 DXCCS18              3   Num  CCS: diagnosis 18
NRD  2013 Core       54 DXCCS19              3   Num  CCS: diagnosis 19
NRD  2013 Core       55 DXCCS20              3   Num  CCS: diagnosis 20
NRD  2013 Core       56 DXCCS21              3   Num  CCS: diagnosis 21
NRD  2013 Core       57 DXCCS22              3   Num  CCS: diagnosis 22
NRD  2013 Core       58 DXCCS23              3   Num  CCS: diagnosis 23
NRD  2013 Core       59 DXCCS24              3   Num  CCS: diagnosis 24
NRD  2013 Core       60 DXCCS25              3   Num  CCS: diagnosis 25
NRD  2013 Core       61 ECODE1               5   Char E code 1
NRD  2013 Core       62 ECODE2               5   Char E code 2
NRD  2013 Core       63 ECODE3               5   Char E code 3
NRD  2013 Core       64 ECODE4               5   Char E code 4
NRD  2013 Core       65 ELECTIVE             2   Num  Elective versus non-elective admission
NRD  2013 Core       66 E_CCS1               4   Num  CCS: E Code 1
NRD  2013 Core       67 E_CCS2               4   Num  CCS: E Code 2
NRD  2013 Core       68 E_CCS3               4   Num  CCS: E Code 3
NRD  2013 Core       69 E_CCS4               4   Num  CCS: E Code 4
NRD  2013 Core       70 FEMALE               2   Num  Indicator of sex
NRD  2013 Core       71 HCUP_ED              2   Num  HCUP Emergency Department service indicator
NRD  2013 Core       72 HOSP_NRD             5   Num  NRD hospital identifier
NRD  2013 Core       73 KEY_NRD             15   Num  NRD record identifier
NRD  2013 Core       74 LOS                  5   Num  Length of stay (cleaned)
NRD  2013 Core       75 MDC                  2   Num  MDC in effect on discharge date
NRD  2013 Core       76 MDC_NoPOA            2   Num  MDC in use on discharge date, calculated without POA
NRD  2013 Core       77 NCHRONIC             2   Num  Number of chronic conditions
NRD  2013 Core       78 NDX                  3   Num  Number of diagnoses on this record
NRD  2013 Core       79 NECODE               3   Num  Number of E codes on this record
NRD  2013 Core       80 NPR                  3   Num  Number of procedures on this record
NRD  2013 Core       81 NRD_DaysToEvent     10   Num  Timing variable used to identify days between admissions
NRD  2013 Core       82 NRD_STRATUM          5   Num  NRD stratum used for weighting
NRD  2013 Core       83 NRD_VisitLink        6   Char NRD visitlink
NRD  2013 Core       84 ORPROC               2   Num  Major operating room procedure indicator
NRD  2013 Core       85 PAY1                 2   Num  Primary expected payer (uniform)
NRD  2013 Core       86 PL_NCHS              3   Num  Patient Location: NCHS Urban-Rural Code
NRD  2013 Core       87 PR1                  4   Char Procedure 1
NRD  2013 Core       88 PR2                  4   Char Procedure 2
NRD  2013 Core       89 PR3                  4   Char Procedure 3
NRD  2013 Core       90 PR4                  4   Char Procedure 4
NRD  2013 Core       91 PR5                  4   Char Procedure 5
NRD  2013 Core       92 PR6                  4   Char Procedure 6
NRD  2013 Core       93 PR7                  4   Char Procedure 7
NRD  2013 Core       94 PR8                  4   Char Procedure 8
NRD  2013 Core       95 PR9                  4   Char Procedure 9
NRD  2013 Core       96 PR10                 4   Char Procedure 10
NRD  2013 Core       97 PR11                 4   Char Procedure 11
NRD  2013 Core       98 PR12                 4   Char Procedure 12
NRD  2013 Core       99 PR13                 4   Char Procedure 13
NRD  2013 Core      100 PR14                 4   Char Procedure 14
NRD  2013 Core      101 PR15                 4   Char Procedure 15
NRD  2013 Core      102 PRCCS1               3   Num  CCS: procedure 1
NRD  2013 Core      103 PRCCS2               3   Num  CCS: procedure 2
NRD  2013 Core      104 PRCCS3               3   Num  CCS: procedure 3
NRD  2013 Core      105 PRCCS4               3   Num  CCS: procedure 4
NRD  2013 Core      106 PRCCS5               3   Num  CCS: procedure 5
NRD  2013 Core      107 PRCCS6               3   Num  CCS: procedure 6
NRD  2013 Core      108 PRCCS7               3   Num  CCS: procedure 7
NRD  2013 Core      109 PRCCS8               3   Num  CCS: procedure 8
NRD  2013 Core      110 PRCCS9               3   Num  CCS: procedure 9
NRD  2013 Core      111 PRCCS10              3   Num  CCS: procedure 10
NRD  2013 Core      112 PRCCS11              3   Num  CCS: procedure 11
NRD  2013 Core      113 PRCCS12              3   Num  CCS: procedure 12
NRD  2013 Core      114 PRCCS13              3   Num  CCS: procedure 13
NRD  2013 Core      115 PRCCS14              3   Num  CCS: procedure 14
NRD  2013 Core      116 PRCCS15              3   Num  CCS: procedure 15
NRD  2013 Core      117 REHABTRANSFER        2   Num  A combined record involving rehab transfer
NRD  2013 Core      118 RESIDENT             2   Num  Patient State is the same as Hospital State
NRD  2013 Core      119 SAMEDAYEVENT         2   Char Transfer flag indicating combination of discharges involve same day events
NRD  2013 Core      120 TOTCHG              10   Num  Total charges (cleaned)
NRD  2013 Core      121 YEAR                 4   Num  Calendar year
NRD  2013 Core      122 ZIPINC_QRTL          2   Num  Median household income national quartile for patient ZIP Code
    """
}

LMAP = {
('source_dict',5):('sourceh','S_ROUTINE',61,'Routine, birth, etc'),
('source_dict',4):('sourceh','S_COURT',68,'Court/Law enforcement'),
('source_dict',2):('sourceh','S_HOSPITAL',62,'Another hospital'),
('source_dict',3):('sourceh','S_OTHER',65,'Another health facility including LTC'),
('source_dict',1):('sourceh','S_ED',66,'Emergency department'),
('source_dict',-21):('sourceh','S_UNKNOWN',67,'Unknown'),
('source_dict',-8):('sourceh','S_UNKNOWN',67,'Unknown'),
('source_dict',-9):('sourceh','S_UNKNOWN',67,'Unknown'),
('source_dict',-1):('sourceh','S_UNKNOWN',67,'Unknown'),
('disposition_dict',1):('disph','D_ROUTINE',71,'Routine'),
('disposition_dict',2):('disph','D_HOSPITAL',72,'Transfer to short-term hospital'),
('disposition_dict',5):('disph','D_OTHER',76,'Transfer other SNF, ICF, etc.'),
('disposition_dict',6):('disph','D_HOME',74,'Home Health Care'),
('disposition_dict',20):('disph','D_DEATH',75,'Died in hospital'),
('disposition_dict',21):('disph','D_COURT',70,'to court/law enforcement'),
('disposition_dict',99):('disph','D_UNKNOWNALIVE',79,'Unknown, Alive'),
('disposition_dict',-8):('disph','D_UNKNOWN',77,'Unknown'),
('disposition_dict',-9):('disph','D_UNKNOWN',77,'Unknown'),
('disposition_dict',7):('disph','D_AMA',78,'Against medical advice'),
('died_dict',0) : ('deathh','ALIVE',30,'Alive'),
('died_dict',1) : ('deathh','DEAD',31,'Died in hospital'),
('died_dict',-8) : ('deathh','DEATH_UNKNOWN',32,'Unknown'),
('died_dict',-9) : ('deathh','DEATH_UNKNOWN',32,'Unknown'),
('sex_dict',0) : ('sexh','MALE',10,'Male'),
('sex_dict',1) : ('sexh','FEMALE',11,'Female'),
('sex_dict',-9) : ('sexh','SEX_UNKNOWN',12,'Sex unknown'),
('sex_dict',-8) : ('sexh','SEX_UNKNOWN',12,'Sex unknown'),
('sex_dict',-6) : ('sexh','SEX_UNKNOWN',12,'Sex unknown'),
('payer_dict',1):('payerh','MEDICARE',41,'Medicare'),
('payer_dict',2):('payerh','MEDICAID',42,'Medicaid'),
('payer_dict',3):('payerh','PRIVATE',43,'Private insurance'),
('payer_dict',4):('payerh','SELF',44,'Self-pay'),
('payer_dict',5):('payerh','FREE',47,'No charge'),
('payer_dict',6):('payerh','OTHER',45,'Other payer'),
('payer_dict',-8):('payerh','P_UNKNOWN',46,'Unknown payer'),
('payer_dict',-9):('payerh','P_UNKNOWN',46,'Unknown payer'),
('race_dict',1):('raceh','WHITE',51,'White'),
('race_dict',2):('raceh','Black',52,'Black'),
('race_dict',3):('raceh','HISPANIC',53,'Hispanic'),
('race_dict',4):('raceh','ASIAN',54,'Asian or Pacific Islander'),
('race_dict',5):('raceh','NATIVE',55,'Native American'),
('race_dict',6):('raceh','R_OTHER',56,'Other'),
('race_dict',-8):('raceh','R_UNKNOWN',57,'Race missing, unknown'),
('race_dict',-9):('raceh','R_UNKNOWN',57,'Race missing, unknown'),
('dnr_dict',-1):('dnrh','DNR_UNAVAILABLE',83,'DNR unavailable'),
('dnr_dict',0):('dnrh','DNR_NO',80,'No DNR'),
('dnr_dict',1):('dnrh','DNR_YES',81,'DNR'),
('dnr_dict',-8):('dnrh','DNR_UNKNOWN',82,'DNR missing'),
('dnr_dict',-9):('dnrh','DNR_UNKNOWN',82,'DNR missing'),
('pzip_dict',1):('pziph','Z_FIRST',101,'First income quartile'),
('pzip_dict',2):('pziph','Z_SECOND',102,'First income quartile'),
('pzip_dict',3):('pziph','Z_THIRD',103,'First income quartile'),
('pzip_dict',4):('pziph','Z_FOURTH',104,'First income quartile'),
('pzip_dict',-9):('pziph','Z_UNKNOWN',105,'Unknown income quartile'),
('pzip_dict',-8):('pziph','Z_UNKNOWN',105,'Unknown income quartile'),
}


FILES = [
    (2013,"NRD_2013_Core.CSV"),
]

PARSERS = {2013:{l.split()[4]:int(l.split()[3])-1 for l in FORMAT["2013"].split("\n") if l.strip()}}
DX_format = re.compile('DX[0-9]{1,2}')
PR_format = re.compile('PR[0-9]{1,2}')
EX_format = re.compile('ECODE[0-9]{1,2}')



def get_zip(d):
    if'MEDINCSTQ' in d:
        return 'MEDINCSTQ'
    elif'ZIPINC_QRTL' in d:
        return 'ZIPINC_QRTL'
    else:
        return None


def parse(line):
    pobj = visit.Patient()
    pobj.raw = line
    pobj.linked = True
    entries = line.split("\t")
    patient_key = entries[0]
    pobj.patient_key = patient_key
    for e in entries[1:]:
        days,year,line = e.split('|SEP|')
        year = int(year)
        vobj = pobj.visits.add()
        process_entry(year,line.strip('\r\n').split(','),vobj)
        visit.index_procedures(vobj)
    visit.sort_visits(pobj)
    return pobj


def process_entry(year,line,vobj):
    vobj.vtype = enums.IP
    parser = PARSERS[year]
    entries= {k:line[v] for k,v in parser.iteritems()}
    vobj.key = entries['KEY_NRD']
    pdx = entries['DX1']
    if pdx.strip():
        vobj.primary_diagnosis = 'D{}'.format(pdx)
    else:
        vobj.primary_diagnosis = ""
    vobj.patient_key = entries['NRD_VisitLink']
    vobj.state = "NRD"
    vobj.day = int(entries['NRD_DaysToEvent'])
    vobj.age = int(entries['AGE'])
    if entries['HCUP_ED'].strip():
        vobj.source = enums.S_ED
    else:
        vobj.source = enums.S_OTHER
    vobj.race = enums.R_UNKNOWN
    vobj.sex = LMAP[('sex_dict',int(entries['FEMALE']))][2]
    vobj.payer = LMAP[('payer_dict',int(entries['PAY1']))][2]
    vobj.disposition = LMAP[('disposition_dict',int(entries['DISPUNIFORM']))][2]
    vobj.death = LMAP[('died_dict',int(entries['DIED']))][2]
    vobj.year = int(year)
    month = entries['DMONTH']
    if month:
        vobj.month = int(month)
    else:
        vobj.month = -1
    vobj.quarter = int(entries['DQTR'])
    charges = float(entries['TOTCHG'])
    vobj.zip = LMAP[('pzip_dict',int(entries['ZIPINC_QRTL']))][2]
    if charges:
        vobj.charge = float(charges)
    else:
        vobj.charge = -1
    vobj.dataset = "NRD_"+str(year)
    vobj.drg = 'DG{}'.format(entries['DRG'])
    los = entries['LOS']
    if los.strip() and los >= 0:
        vobj.los = int(los)
    else:
        vobj.los = -1
    vobj.facility = entries['HOSP_NRD']
    dxlist = [entries['DX{}'.format(k+1)] for k in range(25)]
    prlist = [entries['PR{}'.format(k+1)] for k in range(15)]
    vobj.dnr = enums.DNR_UNAVAILABLE
    for k in dxlist:
        if k.strip():
            vobj.dxs.append("D"+k)
    for k in range(1,5):
        if entries["ECODE{}".format(k)].strip():
            vobj.exs.append("E"+entries["ECODE{}".format(k)])
    primary_procedure = entries['PR1']
    if len(primary_procedure.strip()) > 1:
        vobj.primary_procedure.pcode = 'P'+primary_procedure
        vobj.primary_procedure.ctype = enums.ICD
        vobj.primary_procedure.pday = 0
    for pr in prlist:
        if pr.strip():
            temp = vobj.prs.add()
            temp.pcode = "P"+pr
            temp.pday = -1
            temp.ctype = enums.ICD

def fuzz_entry(entry,vobj):
    """
    Implement an entry fuzzer for generating test data
    :param entry:
    :param vobj:
    :return:
    """
    pass


def process_buffer_hcupnrd(line):
    p = parse(line)
    return p.patient_key,p.SerializeToString()


def finalize_hcupnrd(res,db):
    wb = db.write_batch()
    for patient_key,pstr in res:
        wb.put(("NRD"+patient_key).encode("utf-8"),pstr)
    wb.write()
