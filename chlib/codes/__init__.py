__author__ = 'aub3'

"""
Data from following files is used to create description of codes
http://www.hcup-us.ahrq.gov/db/tools/DRG_Formats.TXT
http://www.hcup-us.ahrq.gov/db/tools/HCUP_Formats.txt
http://www.hcup-us.ahrq.gov/db/tools/I9_Formats.TXT
http://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/codes.html
"""
import gzip,json
from collections import defaultdict
from ..entity.enums import STRINGS

BASE_URL = "/Code"


class Coder(dict):
    """
        # for line in gzip.GzipFile(lfpath+'ICD_Diagnoses.gz'):
        #     CODES_ALL[prefix_icd_diagnosis+line.strip()[:6].strip()].append(line.strip()[6:].strip().decode("utf8","replace"))
        #     CODE_PREFIX[prefix_icd_diagnosis+line.strip()[:6].strip()] = prefix_icd_diagnosis
        # for line in gzip.GzipFile(lfpath+'ICD_Procedures.gz'):
        #     CODES_ALL[prefix_icd_procedures+line.strip()[:5].strip()].append(line.strip()[5:].strip().decode("utf8","replace"))
        #     CODE_PREFIX[prefix_icd_procedures+line.strip()[:5].strip()] = prefix_icd_procedures
    """
    lfpath = __file__.split('__init__')[0]
    CODES_ALL = defaultdict(list)
    CODES_LONG = {}
    CODES_SHORT = {}
    CODE_PREFIX = {}
    prefix_icd_diagnosis = 'D'
    prefix_icd_procedures = 'P'
    prefix_drg = 'DG'
    prefix_cpt = 'C'
    prefix_hcpcs = 'H'
    prefix = {
        'ICD_9_DX': prefix_icd_diagnosis,
        'ICD_9_PR': prefix_icd_procedures,
        'DRG': prefix_drg,
        'CPT': prefix_cpt,
        'HCPCS': prefix_hcpcs,
    }
    prefix_inverse = {
        prefix_icd_diagnosis: 'ICD_9_DX',
        prefix_icd_procedures: 'ICD_9_PR',
        prefix_drg: 'DRG',
        prefix_cpt:'CPT',
        prefix_hcpcs:'HCPCS'
    }
    prefix_name = {
        prefix_icd_diagnosis: 'ICD 9 Diagnosis',
        prefix_icd_procedures: 'ICD 9 Procedure',
        prefix_drg: 'DRG',
        prefix_cpt:'CPT Procedure',
        prefix_hcpcs:'HCPCS'
    }

    for row in json.load(file(lfpath+'strings.json')):
        p,k,s,l = row
        if p != 'G':
            if s.strip():
                CODES_SHORT[k] = s
                CODES_ALL[k].append(s)
            if l.strip():
                CODES_LONG[k] = l
                CODES_ALL[k].append(l)
            CODE_PREFIX[k] = p

    def get_type(self, item):
        if type(item) is int:
            return "Enum"
        else:
            if item.startswith(Coder.prefix_drg):
                return Coder.prefix_name[Coder.prefix_drg]
            elif item.startswith(Coder.prefix_icd_diagnosis):
                return Coder.prefix_name[Coder.prefix_icd_diagnosis]
            elif item.startswith(Coder.prefix_icd_procedures):
                return Coder.prefix_name[Coder.prefix_icd_procedures]
            elif item.startswith(Coder.prefix_cpt):
                return Coder.prefix_name[Coder.prefix_cpt]
            else:
                return "Unknown"

    def __getitem__(self, item):
        if type(item) is int:
            return STRINGS.get(item,"String for enum {} undefined".format(item))
        else:
            if item.startswith('D') or item.startswith('DG') or item.startswith('C') or item.startswith('P') or item.startswith('EE'):
                try:
                    if item.startswith('EE'):
                        item = item.replace('EE','DE')
                    occur_modifier = ""
                    if '+' in item or '_' in item:
                        item,occur = item.replace('_','+').split('+')
                        if occur == '2':
                            occur_modifier = ' 2nd '
                        elif occur == '3':
                            occur_modifier = ' 3rd '
                        else:
                         occur_modifier = ' '+occur+'th '
                    return Coder.CODES_LONG[item]+occur_modifier
                except KeyError:
                    return "Description not found for "+item
            else:
                return item + " (no associated string found)"
                # raise ValueError,item
