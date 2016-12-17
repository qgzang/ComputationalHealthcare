__author__ = 'aub3'
from collections import defaultdict
SUBSET_PLOT_TABLES = ['sexh','sourceh','disph','payerh','raceh']
TABLE_NAMES = {
    'sexh':'Sex',
    'sourceh':'Source',
    'disph':'Disposition',
    'payerh':'Payer',
    'raceh':'Race',
}

def combine_tables(*tables):
    combined = {}
    for i,table in enumerate(tables):
        for k in table:
            combined.setdefault(k.k,len(tables)*[0,])[i] = k.v
    return combined.items()

def combine_dx(*tables):
    combined = {}
    for i,table in enumerate(tables):
        for k in table:
            if k.k not in combined:
                combined[k.k]= {s:[0,0,0] for s in range(len(tables))}
            combined[k.k][i][0] = k.primary
            combined[k.k][i][1] = k.poa
            combined[k.k][i][2] = k.all
    return combined.items()


def default_int():
    return defaultdict(int)

def default_lr():
    return {"left":0,"right":0}

def combine_lr(left,right,left_count,right_count):
    data = defaultdict(default_lr)
    for k in left:
        data[k.k]["left"] = round((100.0*k.v)/left_count,2)
    for k in right:
        data[k.k]["right"] = round((100.0*k.v)/right_count,2)
    return data

def subset_table(subsets):
    rows = []
    rows_set = set()
    data = defaultdict(default_int)
    for i,s in enumerate(subsets):
        data[s.k]['count'] = s.subset.count
        for t in SUBSET_PLOT_TABLES:
            for k in getattr(s.subset,t,[]):
                data[s.k][k.k] = round(100.0*k.v/data[s.k]['count'],1)
                t_s = TABLE_NAMES[t]
                if not (t_s,k.k) in rows_set:
                    rows.append((t_s,k.k))
                    rows_set.add((t_s,k.k))
    return {
               'columns':sorted(data.keys()),
               'data':data,
               'rows':rows
            }

def subset_entry_table(subsets):
    rows = []
    rows_set = set()
    data = defaultdict(default_int)
    for i,s in enumerate(subsets):
        data[s.k]['count'] = s.subset.stats.count
        for t in SUBSET_PLOT_TABLES:
            for k in getattr(s.subset.stats,t,[]):
                data[s.k][k.k] = round(100.0*k.v/data[s.k]['count'],1)
                t_s = TABLE_NAMES[t]
                if not (t_s,k.k) in rows_set:
                    rows.append((t_s,k.k))
                    rows_set.add((t_s,k.k))
    return {
               'columns':sorted(data.keys()),
               'data':data,
               'rows':rows
            }


SUBSET_PLOT_DROPDOWN ={
    "sourceh":{
        61:(61,"Patient was admitted routinely","Source: Routine"),
        62:(62,"Patient was transferred in from another hospital","Source: Hospital"),
        63:(63,"Patient was admitted from SNF","Source: SNF"),
        64:(64,"Patient was admitted from Home Health Care","Source: Home"),
        65:(65,"Patient was admitted from other sources","Source: Other"),
        66:(66,"Patient was admitted via ED","Source: ED"),
        67:(67,"Patient was admitted from unknown source","Source: Unknown"),
        68:(68,"Patient was admitted from court","Source: Court"),
        600:(600,"Patient was admitted from TEXAS PSYCH","Source: TEXAS PSYCH"),
        601:(601,"Patient was admitted from TEXAS CRITICAL","Source: TEXAS CRITICAL"),
        602:(602,"Patient was admitted from TEXAS INTERNAL","Source: TEXAS INTERNAL")
    },
    'disph':{
        71:(71,"Patient was discharged routinely","Disposition: Routine"),
        72:(72,"Patient was discharged to another hospital","Disposition: Hospital"),
        73:(73,"Patient was discharged to SNF","Disposition: SNF"),
        74:(74,"Patient was discharged to Home Health Care ","Disposition: Home"),
        75:(75,"Patient died during the stay","Disposition: Died"),
        76:(76,"Patient disposition other","Disposition: Other"),
        77:(77,"Patient disposition unknown","Disposition: Unknown"),
        78:(78,"Patient dicharged AMA","Disposition: AMA"),
        79:(79,"Patient disposition unknown but alive","Disposition: Unknown but alive"),
    },
    'sexh':{
        10:(10,"Patients were Male","Sex: Male"),
        11:(11,"Patients were Female","Sex: Female"),
        12:(12,"Patients sex unknown","Sex: Unknown"),
    },
    'payerh':{
        41:(41,"Patients with Medicare as payer","Payer: Medicare"),
        42:(42,"Patients with Medicaid as payer","Payer: Medicaid"),
        43:(43,"Patients with Private as payer","Payer: Private"),
        44:(44,"Patients with Self as payer","Payer: Self"),
        45:(45,"Patients with Other as payer","Payer: Other"),
        46:(46,"Patients with unknown payer","Payer: unknown"),
        47:(47,"Patients were not charged ","Payer: Free"),
    }

}


def get_dropdown(plot_data):
    dropdown = {}
    for k in plot_data:
        if k in SUBSET_PLOT_DROPDOWN:
            dropdown[k] = []
            for v in plot_data[k]:
                if v in SUBSET_PLOT_DROPDOWN[k]:
                     dropdown[k].append(SUBSET_PLOT_DROPDOWN[k][v])
    return dropdown



