__author__ = 'aub3'
"""
http://code.activestate.com/recipes/511478-finding-the-percentile-of-the-values/
"""
import pstat_pb2 as stat
import enums
import math
import json
from visit import get_attributes
from collections import defaultdict
import functools
from ..codes import Coder


PROTOCOL = """
package comphealth;
import "penums.proto";
import "pvisit.proto";

message PAGG{
    required string key = 1;
    optional string dataset = 2;
    required bool linked = 3;
    required bool unlinked = 4;
    required int32 patient_count = 6;
    required int32 visit_count = 7;
    required int32 linked_count = 8;
    required int32 unlinked_count = 9;
    repeated PSubsets subsets = 10;
    repeated VisitDeltaHist delta_hist = 11;
    repeated VisitDeltaHist delta_error_hist = 13;
    repeated EtypeCountHist count_hist = 12;
    optional int32 edge_count = 14;
    optional int32 negative_delta_count = 15;
    required Policy policy = 16;
}

message PSubsets{
    required bool linked = 1;
    required ETYPE vtype = 2;
    required AGG subset = 3;
    optional string k = 4;
}

message VisitDeltaHist{
    required ETYPE initial = 1;
    required ETYPE sub = 2;
    required int32 delta = 3;
    required int32 v = 4;
}

message AGG {
    required string key = 1;
    required bool mini = 26 [default = false];
    optional int32 count = 2 [default = 0];
    required Policy policy = 33;
    optional int64 charges_num = 31;
    optional int64 charges_den = 32;
    optional IntHist ageh = 3;
    repeated SexHist sexh = 4;
    repeated RaceHist raceh = 5;
    repeated SourceHist sourceh = 6;
    repeated DispositionHist disph = 7;
    repeated PayerHist payerh = 8;
    repeated DeathHist deathh = 9;
    repeated DXI dxh = 10;
    repeated KVI primary_prh = 11;
    repeated KVI prh = 13;
    repeated KVI exh = 14;
    repeated KVI drgh = 15;
    optional IntHist losh = 16;
    repeated DNRHist dnrh = 18;
    repeated PZipHist pziph = 19;
    repeated KVII agedh = 21;
    repeated KVII yearh = 22;
    repeated EtypeHist vtypeh = 23;
    repeated string facilityh = 24;
    optional string dataset = 25;
    }

message Policy{
   required int32 min_count = 1;
   required int32 min_hospital = 2;
   required int32 base = 3;
   required int32 min_subset = 4;
}


message IntHist{
   repeated KVII h = 1;
   optional int32 median = 2;
   optional int32 fq = 3;
   optional int32 tq = 4;
   optional float mean = 5;
}

message KVII {
    required int32 k = 1;
    required int32 v = 2;
    optional string s = 3;
  }

message DXI {
 required string k = 1;
 optional int32 primary = 2;
 optional int32 poa = 3;
 optional int32 all = 4;
 optional string c = 5;
 optional string s = 6;
}


message DispositionHist {
    required DISPOSITION k = 1;
    required int32 v = 2;
    optional string s = 3;
}

message EtypeHist {
    required ETYPE k = 1;
    required int32 v = 2;
    optional string s = 3;
}

message EtypeCountHist {
    required bool linked = 5;
    required int32 ip = 1;
    required int32 ed = 2;
    required int32 asg = 3;
    required int32 v = 4;
}

message SourceHist {
    required SOURCE k = 1;
    required int32 v = 2;
    optional string s = 3;
}

message PayerHist {
    required PAYER k = 1;
    required int32 v = 2;
    optional string s = 3;
}

message RaceHist {
    required RACE k = 1;
    required int32 v = 2;
    optional string s = 3;
}

message DeathHist {
    required DEATH k = 1;
    required int32 v = 2;
    optional string s = 3;
}


message SexHist {
    required SEX k = 1;
    required int32 v = 2;
    optional string s = 3;
}

message DNRHist {
    required DNR k = 1;
    required int32 v = 2;
    optional string s = 3;
}
message PZipHist {
    required PZIP k = 1;
    required int32 v = 2;
    optional string s = 3;
}
"""

def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1

def compute_stats(entries,allow_negatives = False):
    expanded = []
    numerator = 0.0
    for k in sorted(entries.keys()):
        if k >= 0 or allow_negatives:
            v = entries[k]
            expanded.extend([k]*v)
            numerator += k*v
    return numerator/float(len(expanded)),percentile(expanded, percent=0.5),percentile(expanded, percent=0.25),percentile(expanded, percent=0.75)

def counter():
    return defaultdict(int)

def subset():
    return Aggregate()

class Policy(object):
    def __init__(self,base=20,min_count=60,min_hospital=5,min_subset=200,s=None):
        self.obj = stat.Policy()
        if s:
            self.obj.min_count,self.obj.min_hospital,self.obj.base,self.obj.min_subset = eval(s)
            self.min_count,self.min_hospital,self.base,self.min_subset = eval(s)
        else:
            self.base = base
            self.min_count = min_count
            self.min_hospital = min_hospital
            self.min_subset = min_subset
            self.obj.base = base
            self.obj.min_count = min_count
            self.obj.min_hospital = min_hospital
            self.obj.min_subset = min_subset

    def __eq__(self, other):
        return self.base == other.base and self.min_count == other.min_count and self.min_hospital == other.min_hospital and self.min_subset == other.min_subset

    def __repr__(self):
        return repr((self.obj.min_count,self.obj.min_hospital,self.obj.base,self.obj.min_subset))


def sanitize(v,policy):
    if v > policy.min_count:
        return int(policy.base*int(math.floor(v/float(policy.base))))
    else:
        return 0

def process_exclusions(ex_counter,policy):
    exclusions = {}
    exclusions['selected'] = sanitize(ex_counter['selected'],policy)
    if 'selected_pediatric' in ex_counter:
        exclusions['selected_pediatric'] = sanitize(ex_counter['selected_pediatric'],policy)
    else:
        exclusions['selected_pediatric'] = 0
    exclusions['total'] = sanitize(ex_counter['total'],policy)
    exclusions['excluded'] = sanitize(ex_counter['excluded'],policy)
    exclusions['reasons'] = []
    for k,v in ex_counter.items():
        if k != 'selected' and k != 'selected_pediatric' and k != "total" and k != "excluded":
            v = sanitize(v,policy)
            exclusions['reasons'].append(list(k)+[str(v),])
    return exclusions




class PAggregate(object):
    """
    required string key = 1;
    optional string dataset = 2;
    required bool linked = 3;
    required bool unlinked = 4;
    required int32 patient_count = 6;
    required int32 visit_count = 7;
    required int32 linked_count = 8;
    required int32 unlinked_count = 9;
    repeated PSubsets subsets = 10;
    repeated VisitDeltaHist delta_hist = 11;
    repeated VisitDeltaHist delta_error_hist = 11;
    repeated EtypeCountHist count_hist = 12;
    """
    def __init__(self):
        self.obj = stat.PAGG()
        self.compute_mode = False
        self.base = None
        self.min_count = None
        self.min_hospital = None
        self.subsets = defaultdict(subset)
        self.policy = None

    def init_compute(self,key,dataset,policy,linked=True,unlinked=True):
        self.compute_mode = True
        self.linked_count = 0
        self.unlinked_count = 0
        self.patient_count = 0
        self.visit_count = 0
        self.edge_count = 0
        self.negative_delta_count = 0
        self.obj.key = key
        self.obj.dataset = dataset
        self.obj.linked = linked
        self.obj.unlinked = unlinked
        self.obj.policy.CopyFrom(policy.obj)
        self.base = policy.base
        self.min_count = policy.min_count
        self.min_hospital = policy.min_hospital
        self.vtype_hist = defaultdict(int)
        self.delta_hist = defaultdict(int)
        self.delta_error_hist = defaultdict(int)
        self.policy = policy
        for k,v in enums.ETYPE.items():
            if self.obj.linked:
                self.subsets[(True,v)].init_compute(key,"linked visit type "+k+" "+dataset,policy)
            if self.obj.unlinked:
                self.subsets[(False,v)].init_compute(key,"unlinked visit type "+k+" "+dataset,policy)

    def add_patient(self,p):
        if p.patient_key and not p.patient_key.startswith('-'):
            linked = True
            self.patient_count += 1
            self.linked_count += len(p.visits)
            self.visit_count += len(p.visits)
        else:
            linked = False
            self.unlinked_count += 1
            self.visit_count += 1
        tempcounter = defaultdict(int)
        for i,v in enumerate(p.visits):
            tempcounter[v.vtype] += 1
            self.subsets[(linked,v.vtype)].add(v)
            if i+1 != len(p.visits):
                self.edge_count += 1
                n = p.visits[i+1]
                delta = n.day - (v.day + v.los)
                if delta>=0 :
                    self.delta_hist[(v.vtype,n.vtype,delta)] += 1
                else:
                    self.negative_delta_count += 1
                    if delta == -1:
                        self.delta_error_hist[(v.vtype,n.vtype,-1)] += 1
                    else:
                        self.delta_error_hist[(v.vtype,n.vtype,-2)] += 1
        self.vtype_hist[(linked,tempcounter[enums.IP],tempcounter[enums.ED],tempcounter[enums.AS])] += 1

    def end_compute(self):
        if self.visit_count > self.min_count:
            self.obj.patient_count = self.patient_count
            self.obj.linked_count = self.linked_count
            self.obj.unlinked_count = self.unlinked_count
            self.obj.visit_count = self.visit_count
            self.obj.edge_count = self.edge_count
            self.obj.negative_delta_count = self.negative_delta_count
            for k,v in self.vtype_hist.iteritems():
                if v >= self.min_count:
                    temp = self.obj.count_hist.add()
                    temp.linked = k[0]
                    temp.ip = k[1]
                    temp.ed = k[2]
                    temp.asg = k[3]
                    temp.v = v
            for k,v in self.delta_hist.iteritems():
                if v >= self.min_count:
                    temp = self.obj.delta_hist.add()
                    temp.initial = k[0]
                    temp.sub = k[1]
                    temp.delta = k[2]
                    temp.v = v
            for k,v in self.delta_error_hist.iteritems():
                if v >= self.min_count:
                    temp = self.obj.delta_error_hist.add()
                    temp.initial = k[0]
                    temp.sub = k[1]
                    temp.delta = k[2]
                    temp.v = v
            for k,v in self.subsets.iteritems():
                if v.end_compute():
                    temp = self.obj.subsets.add()
                    temp.linked = k[0]
                    temp.vtype = k[1]
                    temp.subset.CopyFrom(v.obj)
            return True
        else:
            return False

    def pause_compute(self):
        data = {}
        data['linked'] = self.obj.linked
        data['unlinked'] = self.obj.unlinked
        data['linked_count'] = self.linked_count
        data['patient_count'] = self.patient_count
        data['unlinked_count'] = self.unlinked_count
        data['visit_count'] = self.visit_count
        data['key'] = self.obj.key
        data['dataset'] = self.obj.dataset
        data['policy'] = repr(self.policy)
        data['vtype_hist'] = self.vtype_hist
        data['delta_hist'] = self.delta_hist
        data['delta_error_hist'] = self.delta_error_hist
        data['edge_count'] = self.edge_count
        data['negative_delta_count'] = self.negative_delta_count
        data['subsets'] = {}
        for k,v in self.subsets.iteritems():
            data['subsets'][repr(k)] = v.pause_compute()
        return data



    def resume_compute(self,data):
        if self.policy is None and self.base is None and self.min_count is None and self.min_hospital is None:
            self.policy = Policy(s=data['policy'])
            self.init_compute(data['key'],data['dataset'],self.policy,data['linked'],data['unlinked'])
        if self.policy == Policy(s=data['policy']):
            self.linked_count += data['linked_count']
            self.unlinked_count += data['unlinked_count']
            self.patient_count += data['patient_count']
            self.visit_count += data['visit_count']
            self.edge_count += data['edge_count']
            self.negative_delta_count += data['negative_delta_count']

            for k,v in data['delta_hist'].iteritems():
                self.delta_hist[k] += v
            for k,v in data['delta_error_hist'].iteritems():
                self.delta_error_hist[k] += v
            for k,v in data['vtype_hist'].iteritems():
                self.vtype_hist[k] += v
            for k,v in data['subsets'].iteritems():
                self.subsets[eval(k)].obj.policy.CopyFrom(self.policy.obj)
                self.subsets[eval(k)].resume_compute(v)
        else:
            raise ValueError,str((self.obj.policy.base,data['base'],self.obj.min_count,data['min_count'],self.obj.min_hospital,data['min_hospital']))

    def ParseFromString(self,s):
        self.obj.ParseFromString(s)

    def SerializeToString(self):
        return self.obj.SerializeToString()




class Aggregate(object):
    int_types = set(['ageh','yearh','losh'])

    def __init__(self,mini=False):
        self.obj = stat.AGG()
        self.count = None
        self.base = None
        self.compute_mode = False
        self.charges_num = None
        self.charges_den = None
        self.counter = None
        self.counter_hospitals = None
        self.min_count = None
        self.min_subset = None
        self.min_hospital =  None
        self.mini = mini
        self.policy = None


    def init_compute(self,key,dataset,policy):
        self.count = 0
        self.compute_mode = True
        self.policy = policy
        self.obj.policy.CopyFrom(policy.obj)
        self.obj.key = key
        self.obj.dataset = dataset
        self.base = float(policy.base)
        self.min_count = policy.min_count
        self.min_subset = policy.min_subset
        self.min_hospital =  policy.min_hospital
        self.counter = defaultdict(int)
        self.counter_hospitals = defaultdict(set)
        self.charges_num = 0
        self.charges_den = 0


    def add_k(self,k,facility):
        self.counter[k] += 1
        if not(self.counter_hospitals[k] is None):
            self.counter_hospitals[k].add(facility)
            if len(self.counter_hospitals[k]) > self.min_hospital:
                self.counter_hospitals[k] = None


    def add(self,visit):
        self.count += 1
        if visit.charge >= 0:
            self.charges_num += int(visit.charge)
            self.charges_den += 1

        for k in get_attributes(visit):
            self.add_k(k,visit.facility)
        for k in [('ageh',visit.age),('agedh',self.discrete_age(visit.age)),('yearh',visit.year),('losh',visit.los),('fachilityh',visit.facility)]:
                self.add_k(k,visit.facility)

        if not self.mini:
            for field,codes in [('exh',visit.exs),('dx',visit.dxs),('dx_poa',visit.poas),('dx_prim',[visit.primary_diagnosis,]),('drgh',[visit.drg,])]:
                for code in codes:
                    self.add_k((field,code),visit.facility)
            for field,codes in [('primary_prh',[visit.primary_procedure]),('prh',visit.prs)]:
                for code in codes:
                    self.add_k((field,code.pcode),visit.facility)


    def pause_compute(self):
        data = {}
        data['count'] = self.count
        data['base'] = self.obj.policy.base
        data['key'] = self.obj.key
        data['dataset'] = self.obj.dataset
        data['min_count'] = self.min_count
        data['min_hospital'] = self.min_hospital
        data['policy'] = repr(self.policy)
        data['counter'] = self.counter
        data['counter_hospitals'] = self.counter_hospitals
        data['charges_num'] = self.charges_num
        data['charges_den'] = self.charges_den
        return data

    def resume_compute(self,data):
        if self.compute_mode:
            if 'policy' not in data:
                data['policy'] = repr(Policy()) # if policy is missing use default policy
            temp_policy = Policy(s=data['policy'])
            if self.policy is None:
                self.policy = temp_policy
                self.obj.policy.CopyFrom(temp_policy.obj)
            if self.policy == temp_policy:
                self.count += data["count"]
                self.charges_num += data['charges_num']
                self.charges_den += data['charges_den']
                for k,v in data['counter'].iteritems():
                    self.counter[k] += v
                for k,v in data['counter_hospitals'].iteritems():
                    if self.counter_hospitals[k] is None:
                        self.counter_hospitals[k] = None
                    elif v is None:
                        self.counter_hospitals[k] = None
                    else:
                        self.counter_hospitals[k] = self.counter_hospitals[k].union(v)
                        if len(self.counter_hospitals[k]) > self.min_hospital:
                            self.counter_hospitals[k] = None


    def discrete_age(self,age):
        return int(20*math.floor(age/20.0))

    def compute_stats(self,entries,allow_negatives = False):
        expanded = []
        numerator = 0.0
        for k in sorted(entries.keys()):
            if k >= 0 or allow_negatives:
                v = entries[k]
                expanded.extend([k]*v)
                numerator += k*v
        return numerator/float(len(expanded)),percentile(expanded, percent=0.5),percentile(expanded, percent=0.25),percentile(expanded, percent=0.75)

    def end_compute(self):
        if self.count > self.min_count and self.count > self.min_subset:
            self.obj.mini = self.mini
            self.obj.count = int(self.base*int(math.floor(self.count/float(self.base))))
            if self.charges_den > self.min_count:
                self.obj.charges_num = self.charges_num
                self.obj.charges_den = self.charges_den
            combined_dx = defaultdict(lambda :{'primary':0,'poa':0,'all':0})
            int_histogram = defaultdict(dict)
            for k,v in self.counter.iteritems():
                if type(k) is tuple:
                    try:
                        if k[0] == 'losh' or k[0] == 'ageh':
                            int_histogram[k[0]][k[1]] = v
                    except:
                        raise ValueError,k
                if v > self.min_count and self.counter_hospitals[k] is None:
                    if type(k) is int:
                        # try:
                        temp = self.obj.__getattribute__(enums.INTMAP[k]).add()
                        temp.k = k
                        temp.v = int(self.base*int(math.floor(v/float(self.base)))) if v > self.min_count else 0
                        # except:
                        #     raise ValueError,(k,v,schema.INTMAP)
                    elif not k[0].startswith('dx') and k[0] not in Aggregate.int_types:
                        temp = self.obj.__getattribute__(k[0]).add()
                        temp.k = k[1]
                        temp.v = int(self.base*int(math.floor(v/float(self.base)))) if v > self.min_count else 0
                    elif k[0] == 'dx_prim':
                        combined_dx[k[1]]['primary'] = int(self.base*int(math.floor(v/float(self.base)))) if v > self.min_count else 0
                    elif k[0] == 'dx_poa':
                        combined_dx[k[1]]['poa'] = int(self.base*int(math.floor(v/float(self.base)))) if v > self.min_count else 0
                    elif k[0] == 'dx':
                        combined_dx[k[1]]['all'] = int(self.base*int(math.floor(v/float(self.base)))) if v > self.min_count else 0
            for k,v in int_histogram.iteritems():
                mean,median,fq,tq = compute_stats(v)
                self.obj.__getattribute__(k).median = int(round(median))
                self.obj.__getattribute__(k).mean = round(mean,2)
                self.obj.__getattribute__(k).fq = int(round(fq))
                self.obj.__getattribute__(k).tq = int(round(tq))
                for value,c in v.iteritems():
                    if value >= 0 and c > self.min_count:
                        temp = self.obj.__getattribute__(k).h.add()
                        temp.k = value
                        temp.v = int(self.base*int(math.floor(c/float(self.base)))) if c > self.min_count else 0
            for k,v in combined_dx.iteritems():
                temp = self.obj.dxh.add()
                temp.k = k
                temp.primary = v['primary']
                temp.poa = v['poa']
                temp.all = v['all']
            return True
        else:
            return False


    def age_plot(self):
        """
        Helper function for generating plots
        :return:
        """
        return age_plot(self.obj)


    def los_plot(self):
        """
        Helper function for generating plots
        :return:
        """
        return los_plot(self.obj)

    def export(self,sep="\t"):
        """
        Export aggregate to CSV
        """
        return export(self.obj,sep)

    def __str__(self):
        return self.obj.__str__()

    def __repr__(self):
        return self.obj.SerializeToString()

    def ParseFromString(self,s):
        self.obj.ParseFromString(s)

def export(ag,sep="\t"):
    lines = [sep.join(["count",str(ag.count)]),]
    lines.append("")
    for table in set(enums.INTMAP.values()):
        lines.append(enums.TABLE_STRINGS[table] +"\n" + sep.join(["attribute","count"]))
        found = False
        for e in ag.__getattribute__(table):
            lines.append(sep.join([enums.STRINGS[e.k] if e.k in enums.STRINGS else str(e.k),str(e.v)]))
            found = True
        if not found:
            lines.pop()
        else:
            lines.append(" ")
    lines.append(sep.join(["Age Mean",str(ag.ageh.mean)]))
    lines.append(sep.join(["Age First Quartile",str(ag.ageh.fq)]))
    lines.append(sep.join(["Age Median",str(ag.ageh.median)]))
    lines.append(sep.join(["Age Third Quartile",str(ag.ageh.tq)]))
    lines.append("Age distribution\n"+ sep.join(["Age","count"]))
    for e in ag.ageh.h:
        lines.append(sep.join([str(e.k),str(e.v)]))
    lines.append("")
    lines.append(sep.join(["Length of Stay Mean",str(ag.losh.mean)]))
    lines.append(sep.join(["Length of Stay First Quartile",str(ag.losh.fq)]))
    lines.append(sep.join(["Length of Stay Median",str(ag.losh.median)]))
    lines.append(sep.join(["Length of Stay Third Quartile",str(ag.losh.tq)]))
    lines.append("Length of Stay distribution\n"+ sep.join(["Days","count"]))
    for e in ag.losh.h:
        lines.append(sep.join([str(e.k),str(e.v)]))
    lines.append("")
    lines.append("Procedures\n" + sep.join(["code","count"]))
    for e in ag.prh:
        lines.append(sep.join([e.k.replace('P',''),str(e.v)]))
    lines.append("")
    lines.append("DRG\n" + sep.join(["code","count"]))
    for e in ag.drgh:
        lines.append(sep.join([e.k.replace('DG',''),str(e.v)]))
    lines.append("")
    lines.append("Diagnoses\n" + sep.join(["code","Primary","All"]))
    for e in ag.dxh:
        lines.append(sep.join([e.k.replace('D',''),str(e.primary),str(e.all)]))
    lines.append("")




    return "\n".join(lines) + "\n"


def age_plot(agg):
    """
    Helper function for generating plots
    :return:
    """
    age_plot = { k:0 for k in range(20,100)}
    age_plot.update({t.k:t.v for t in agg.ageh.h})
    return json.dumps(age_plot.items())



def los_plot(agg):
    """
    Helper function for generating plots
    :return:
    """
    los_plot = { k:0 for k in range(0,max([t.k for t in agg.losh.h])+5)}
    los_plot.update({t.k:t.v for t in agg.losh.h})
    return json.dumps(los_plot.items())




def get_value(entry,field,k,default=0):
    for f in entry.__getattribute__(field):
        if f.k == k:
            return f.v
    return default
