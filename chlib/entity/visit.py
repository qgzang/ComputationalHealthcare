__author__ = 'aub3'
import pvisit_pb2
import penums_pb2
import enums
from pvisit_pb2 import Patient,PatientList,VisitEdge
from collections import defaultdict



def read_patient_list(fname):
    p = pvisit_pb2.PatientList()
    p.ParseFromString(file(fname).read())
    return p

def read_visit_list(fname):
    v = pvisit_pb2.VisitList()
    v.ParseFromString(file(fname).read())
    return v





def get_attributes(pvisit):
    return [pvisit.sex,
              pvisit.vtype,
              pvisit.race,
              pvisit.source,
              pvisit.disposition,
              pvisit.dnr,
              pvisit.death,
              pvisit.payer,
              pvisit.zip]



class Visit(object):
    """
    TODO: Marked for Deletion
    """
    def __init__(self):
        self.obj = None

    def ParseFromString(self,s):
        self.obj.ParseFromString(s)

def get_edges(p,visit_types = None):
    if len(p.visits) > 1:
        if visit_types is None:
            visit_types = {enums.AS,enums.IP,enums.ED}
        visits = sorted([(v.day,v.los,-1*v.vtype,v) for v in p.visits if v.vtype in visit_types]) # sorting and filtering use los to ensure shorter visits appears first
        for i,vtuple in enumerate(visits):
            vday,vlos,vtype,v = vtuple
            if i+1 < len(visits):
                yield v,visits[i+1][3]



def sort_visits(p):
    if len(p.visits) > 1:
        sorted_visits = []
        visits = sorted([(v.day,v.los,-1*v.vtype,v) for v in p.visits]) # sorting and filtering use los to ensure shorter visits appears first, also ED visits appear first when ED -> IP transition is present
        del p.visits[:]
        for i,vtuple in enumerate(visits):
            vday,vlos,vtype,v = vtuple
            sorted_visits.append(v)
        p.visits.extend(sorted_visits)


# def get_all_edges(p,visit_types = None):
#     if len(p.visits) > 1:
#         if visit_types is None:
#             visit_types = {enums.AS,enums.IP,enums.ED}
#         visits = sorted([(v.day,v.los,-1*v.vtype,v) for v in p.visits if v.vtype in visit_types]) # sorting and filtering use los to ensure shorter visits appears first
#         for i,vtuple in enumerate(visits):
#             vday,vlos,vtype,v = vtuple
#             vlist = {enums.AS,enums.IP,enums.ED}
#             for stuple in visits[i+1:]:
#                 sday,slos,stype,s = stuple
#                 if vlist and stype in vlist:
#                     vlist.remove(stype)
#                     yield v,s




def index_procedures(visit_obj):
    """
    Index repeated procedures.
    :param visit_obj:
    :return:
    """
    index = defaultdict(list)
    for k in visit_obj.prs:
        index[k.pcode].append((k.pday,k.pcode,k.ctype))
    visit_obj.ClearField("prs")
    for k,v in index.iteritems():
        for i,ptuple in enumerate(sorted(v)):
            pday,pcode,ctype = ptuple
            temp = visit_obj.prs.add()
            if i != 0:
                temp.pcode = pcode + '_' + str(i+1)
            else:
                temp.pcode = pcode
            temp.ctype = ctype
            temp.occur = i+1
            temp.pday = pday
