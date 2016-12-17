from collections import defaultdict,namedtuple
from ..entity.enums import IP,ED,AS
VisitEdge = namedtuple("VisitEdge",['initial','sub'])


def default_set():
    return defaultdict(set)


def get_code_type(code):
    if code.startswith('P') or code.startswith('C'):
        return 'pr'
    elif code.startswith('DG'):
        return 'drg'
    elif code.startswith('D'):
        return 'dx'
    elif code.startswith('E'):
        return 'ex'
    else:
        raise ValueError, "{} could node determine the code type".format(code)


class Extractor(object):

    def __init__(self,codes,dataset,compute_inpatient_visits=True, compute_edges=True):
        self.codes = codes
        self.dataset = dataset
        self.compute_inpatient_visits = compute_inpatient_visits
        self.compute_edges = compute_edges

    def extract(self):
        self.codes = set(self.codes)
        self.ip_visits = defaultdict(set)
        self.rv_nodes = defaultdict(set)
        self.ra_nodes = defaultdict(set)
        self.rv_nodes_un = defaultdict(set)
        self.ra_nodes_un = defaultdict(set)
        self.ra_edges = defaultdict(set)
        self.rv_edges = defaultdict(set)
        self.patients = defaultdict(default_set)
        self.visits = {}
        for code in self.codes:
            code_type = get_code_type(code)
            for pkey, p in self.dataset.iter_patients_by_code(code):
                v_seq_all, v_seq_ip = [], []
                index_visits, index_visits_ip = set(), set()
                self.patients[code][pkey] = p
                for v in p.visits:
                    pv = (pkey, v.key)
                    self.visits[pv] = v
                    if p.linked:
                        v_seq_all.append((v.day, v.los, v.vtype,
                                          v.key))  # check if this is correct logic for ED to IP transition
                        if v.vtype == IP:
                            v_seq_ip.append((v.day, v.los, v.vtype, v.key))
                    if (code_type == 'pr' and [pr.pcode for pr in v.prs if code == pr.pcode]) or \
                            (code_type == 'dx' and v.primary_diagnosis == code) or \
                            (code_type == 'drg' and v.drg == code):
                        if self.compute_inpatient_visits and v.vtype == IP:
                            self.ip_visits[code].add(pv)
                        if self.compute_edges:
                            if p.linked:
                                self.rv_nodes[code].add(pv)
                                index_visits.add(v.key)
                            else:
                                self.rv_nodes_un[code].add(pv)
                            if v.vtype == IP:
                                if p.linked:
                                    self.ra_nodes[code].add(pv)
                                    index_visits_ip.add(v.key)
                                else:
                                    self.ra_nodes_un[code].add(pv)
                if self.compute_edges and len(v_seq_all) > 1:
                    v_seq_all.sort()
                    v_seq_ip.sort()
                    for i, sub in enumerate(v_seq_all[1:]):
                        initial = v_seq_all[i]
                        if initial[3] in index_visits:
                            self.rv_edges[code].add(((pkey, initial[3]), (pkey, sub[3])))
                    for i, sub in enumerate(v_seq_ip[1:]):
                        initial = v_seq_ip[i]
                        if initial[3] in index_visits_ip:
                            self.ra_edges[code].add(((pkey, initial[3]), (pkey, sub[3])))

    def get_inpatient_visits(self,code):
        return [self.visits[k] for k in self.ip_visits[code]]

    def get_readmit_nodes_edges(self,code):
        unlinked_nodes = [self.visits[pv] for pv in self.ra_nodes_un[code]]
        nodes = [self.visits[pv] for pv in self.ra_nodes[code]]
        edges = [VisitEdge(self.visits[pvi], self.visits[pvs]) for pvi, pvs in self.ra_edges[code]]
        return unlinked_nodes,nodes,edges

    def get_revisit_nodes_edges(self,code):
        subsets = defaultdict(dict)
        if code.startswith('P'):
             subsets[code]['unlinked_nodes'] = [self.visits[pv] for pv in self.rv_nodes_un[code]]
             subsets[code]['nodes'] = [self.visits[pv] for pv in self.rv_nodes[code]]
             subsets[code]['edges'] = [VisitEdge(self.visits[pvi], self.visits[pvs]) for pvi, pvs in self.rv_edges[code]]
        elif code.startswith('DG'):
            subsets[code]['unlinked_nodes'] = [self.visits[pv] for pv in self.rv_nodes_un[code]]
            subsets[code]['nodes'] = [self.visits[pv] for pv in self.rv_nodes[code]]
            subsets[code]['edges'] = [VisitEdge(self.visits[pvi], self.visits[pvs]) for pvi, pvs in self.rv_edges[code]]
        elif code.startswith('D'):
            subcode = 'N3DX_IP_{}'.format(code)
            subsets[subcode]['unlinked_nodes'] = [self.visits[pv] for pv in self.rv_nodes_un[code] if self.visits[pv].vtype == IP]
            subsets[subcode]['nodes'] = [self.visits[pv] for pv in self.rv_nodes[code] if self.visits[pv].vtype == IP]
            subsets[subcode]['edges'] = [VisitEdge(self.visits[pvi], self.visits[pvs]) for pvi, pvs in self.rv_edges[code] if self.visits[pvi].vtype == IP]
            subcode = 'N3DX_ED_{}'.format(code)
            subsets[subcode]['unlinked_nodes'] = [self.visits[pv] for pv in self.rv_nodes_un[code] if self.visits[pv].vtype == ED]
            subsets[subcode]['nodes'] = [self.visits[pv] for pv in self.rv_nodes[code] if self.visits[pv].vtype == ED]
            subsets[subcode]['edges'] = [VisitEdge(self.visits[pvi], self.visits[pvs]) for pvi, pvs in self.rv_edges[code] if self.visits[pvi].vtype == ED]
            subcode = 'N3DX_AS_{}'.format(code)
            subsets[subcode]['unlinked_nodes'] = [self.visits[pv] for pv in self.rv_nodes_un[code] if self.visits[pv].vtype == AS]
            subsets[subcode]['nodes'] = [self.visits[pv] for pv in self.rv_nodes[code] if self.visits[pv].vtype == AS]
            subsets[subcode]['edges'] = [VisitEdge(self.visits[pvi], self.visits[pvs]) for pvi, pvs in self.rv_edges[code] if self.visits[pvi].vtype == AS]
        elif code.startswith('C'):
            subcode = 'N3PR_ED_{}'.format(code)
            subsets[subcode]['unlinked_nodes'] = [self.visits[pv] for pv in self.rv_nodes_un[code] if self.visits[pv].vtype == ED]
            subsets[subcode]['nodes'] = [self.visits[pv] for pv in self.rv_nodes[code] if self.visits[pv].vtype == ED]
            subsets[subcode]['edges'] = [VisitEdge(self.visits[pvi], self.visits[pvs]) for pvi, pvs in self.rv_edges[code] if self.visits[pvi].vtype == ED]
            subcode = 'N3PR_AS_{}'.format(code)
            subsets[subcode]['unlinked_nodes'] = [self.visits[pv] for pv in self.rv_nodes_un[code] if self.visits[pv].vtype == AS]
            subsets[subcode]['nodes'] = [self.visits[pv] for pv in self.rv_nodes[code] if self.visits[pv].vtype == AS]
            subsets[subcode]['edges'] = [VisitEdge(self.visits[pvi], self.visits[pvs]) for pvi, pvs in self.rv_edges[code] if self.visits[pvi].vtype == AS]
        return subsets

    def get_patients(self,code):
        return self.patients[code].values()