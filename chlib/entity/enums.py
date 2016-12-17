__author__ = 'aub3'
import penums_pb2
from penums_pb2 import *
import google
enum_list = [(k,vstr,vval) for k,v in penums_pb2.__dict__.iteritems() if type(v) is google.protobuf.internal.enum_type_wrapper.EnumTypeWrapper for vstr,vval in v.items()]

STRINGS = {0: 'Inpatient stay',
           1: 'ED visit',
           2: 'Ambulatory Surgery',
           10: 'Male',
           11: 'Female',
           12: 'Sex unknown',
           30: 'Alive',
           31: 'Died in hospital',
           32: 'Unknown',
           41: 'Medicare',
           42: 'Medicaid',
           43: 'Private insurance',
           44: 'Self-pay',
           45: 'Other payer',
           46: 'Unknown payer',
           47: 'No charge',
           51: 'White',
           52: 'Black',
           53: 'Hispanic',
           54: 'Asian or Pacific Islander',
           55: 'Native American',
           56: 'Other',
           57: 'Race missing, unknown',
           61: 'Routine, birth, etc',
           62: 'Another hospital',
           65: 'Another health facility including LTC',
           66: 'Emergency department',
           67: 'Unknown',
           68: 'Court/Law enforcement',
           70: 'to court/law enforcement',
           71: 'Routine',
           72: 'Transfer to short-term hospital',
           74: 'Home Health Care',
           75: 'Died in hospital',
           76: 'Transfer other SNF, ICF, etc.',
           77: 'Unknown',
           78: 'Against medical advice',
           79: 'Unknown, Alive',
           80: 'No DNR',
           81: 'DNR',
           82: 'DNR missing',
           83: 'DNR unavailable',
           101: 'First income quartile',
           102: 'First income quartile',
           103: 'First income quartile',
           104: 'First income quartile',
           105: 'Unknown income quartile'}


INTMAP = {0: 'vtypeh',
         1: 'vtypeh',
         2: 'vtypeh',
         10: 'sexh',
         11: 'sexh',
         12: 'sexh',
         30: 'deathh',
         31: 'deathh',
         32: 'deathh',
         41: 'payerh',
         42: 'payerh',
         43: 'payerh',
         44: 'payerh',
         45: 'payerh',
         46: 'payerh',
         47: 'payerh',
         51: 'raceh',
         52: 'raceh',
         53: 'raceh',
         54: 'raceh',
         55: 'raceh',
         56: 'raceh',
         57: 'raceh',
         61: 'sourceh',
         62: 'sourceh',
         65: 'sourceh',
         66: 'sourceh',
         67: 'sourceh',
         68: 'sourceh',
         70: 'disph',
         71: 'disph',
         72: 'disph',
         74: 'disph',
         75: 'disph',
         76: 'disph',
         77: 'disph',
         78: 'disph',
         79: 'disph',
         80: 'dnrh',
         81: 'dnrh',
         82: 'dnrh',
         83: 'dnrh',
         101: 'pziph',
         102: 'pziph',
         103: 'pziph',
         104: 'pziph',
         105: 'pziph'}


TABLE_STRINGS = {
    "vtypeh":"Visit Types ",
    "sexh":"Sex distribution ",
    "deathh":"Mortality",
    "payerh":"Payer",
    "raceh":"Race",
    "sourceh":"Admission Source",
    "disph":"Disposition Destination",
    "dnrh":"DNR status (only CA)",
    "pziph":"Income Quartile",
}


for v in SOURCE.values():
    INTMAP[v] ='sourceh'
for v in DISPOSITION.values():
    INTMAP[v] ='disph'
for v in PAYER.values():
    INTMAP[v] ='payerh'
for v in RACE.values():
    INTMAP[v] ='raceh'






