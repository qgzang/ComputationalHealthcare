import glob,plyvel,gzip,time,os,subprocess,json,logging,tempfile,sys,shutil
from .. import formats
import pprint
from ..entity import stream_pb
from ..entity.enums import IP,ETYPE
from ..entity.visit import Patient
from multiprocessing import Pool
from collections import defaultdict,namedtuple
CPP = os.path.abspath(os.path.join(os.path.dirname(__file__),'../cppcode/bin/Debug/cpp'))
import extractor
# from .. import aggregate_visits,aggregate_patients,aggregate_edges

CountTuple = namedtuple('CountTuple','state year linked vtype ctype code count')


def default_int():
    return defaultdict(int)


class Code(object):

    def __init__(self,code,dataset):
        self.code = code
        self.dataset = dataset
        self.counts = []
        self.years = set()
        self.states = set()
        self.ctypes = set()
        self.vtypes = set()
        if self.code.startswith('P') or self.code.startswith('C'):
            self.code_type = 'pr'
        elif self.code.startswith('DG'):
            self.code_type ='drg'
        elif self.code.startswith('D'):
            self.code_type = 'dx'
        elif self.code.startswith('E'):
            self.code_type = 'ex'
        else:
            raise ValueError, "{} could node determine the code type".format(self.code)

    def add_count(self,state, year, linked, vtype, ctype, code, count):
        if code == self.code:
            self.counts.append(CountTuple(code=code,state=state,year=year, linked=linked, vtype=vtype, ctype=ctype, count=count))
            self.states.add(state)
            self.years.add(year)
            self.ctypes.add(ctype)
            self.vtypes.add(vtype)
        else:
            raise KeyError,"{} != {}".format(code,self.code)

    def visits_count(self):
        return sum([k.count for k in self.counts if k.state == 'ALL' and k.ctype != 'pdx'])

    def visits_counts_by_year(self):
        by_year = defaultdict(int)
        for k in self.counts:
            if k.state == 'ALL' and k.ctype != 'pdx':
                by_year[k.year] += k.count
        return by_year

    def visits_counts_by_state(self):
        by_state = defaultdict(int)
        for k in self.counts:
            if k.state != 'ALL' and k.ctype != 'pdx':
                by_state[k.state] += k.count
        return by_state

    def visits_counts_by_visit_type(self):
        by_visit_type = defaultdict(int)
        for k in self.counts:
            if k.state == 'ALL' and k.ctype != 'pdx':
                by_visit_type[k.vtype] += k.count
        return by_visit_type

    def visits_count_primary(self):
        if self.code.startswith('D'):
            return sum([k.count for k in self.counts if k.state == 'ALL' and k.ctype == 'pdx'])
        else:
            raise NotImplementedError

    def get_patients_keys(self):
        return self.dataset.get_patient_keys_by_code(self.code)

    def get_patients(self):
        pass

    def __str__(self):
        return "Code: {} in Dataset: {}".format(self.code,self.dataset.name)\
               + "\nvisits {}".format(self.visits_count())\
               + "\nBy years:\n{}".format(pprint.pformat(self.visits_counts_by_year().items(),indent=2))\
               + "\nBy states:\n{}".format(pprint.pformat(self.visits_counts_by_state().items(),indent=2))\
               + "\nBy visit type:\n{}".format(pprint.pformat(self.visits_counts_by_visit_type().items(),indent=2))

    def get_results(self):
        results = {}
        for fname in glob.glob(self.dataset.result_dir+'/*{}*.json'.format(self.code)):
            results[fname] = json.loads(file(fname).read())
        return results


class Data(object):

    @classmethod
    def test(cls,json_config_path):
        logging.info("starting tests with {}".format(json_config_path))
        datasets = cls.load_from_config(json_config_path)

    @classmethod
    def load_from_config(cls, json_config_path):
        datasets = {}
        with open(json_config_path) as fh:
            config = json.load(fh)
            for k in config['DATASETS']:
                dconfg= config['DATASETS'][k]
                if os.path.isdir(dconfg["ROOT"]):
                    basedir = dconfg["ROOT"]
                    datasets[k] = Data(k,name=dconfg['NAME'],linked=dconfg['LINKED'],base_dir=basedir,years=dconfg['YEARS'],
                                   states=dconfg['STATES'],json_config=dconfg)
        return datasets

    @classmethod
    def get_from_config(cls, json_config_path, dataset_id):
        with open(json_config_path) as fh:
            config = json.load(fh)
            dconfg = config['DATASETS'][dataset_id]
            if os.path.isdir(dconfg["ROOT"]):
                basedir = dconfg["ROOT"]
                logging.info("using {}".format(basedir))
                return Data(dataset_id,name=dconfg['NAME'],linked=dconfg['LINKED'],base_dir=basedir,years=dconfg['YEARS'],
                            states=dconfg['STATES'],json_config=dconfg)
            else:
                raise KeyError

    def __init__(self,identifier,name,linked,base_dir,years,states,json_config):
        self.linked = linked
        self.name = name
        self.identifier = identifier
        self.base_dir = base_dir
        self.asc_dir = os.path.join(self.base_dir, 'ASC/')
        self.raw_dir = os.path.join(self.base_dir,'RAW/')
        self.db = os.path.join(self.base_dir,'DB/')
        self.result_dir = os.path.join(self.base_dir,'RESULT/')
        self.code_index_dir = os.path.join(self.base_dir,'CINDEX/')
        self.N4 = os.path.join(self.base_dir, 'N4/')
        self.code_index_db = os.path.join(self.base_dir, 'CINDEXDB/')
        self.states = states
        self.years = years
        self.state_counts = defaultdict(default_int)
        self.type_counts = {(state,v,l):0 for state in self.states for k,v in ETYPE.items() for l in ["linked","unlinked"]}
        self.year_counts = {(state,v,year,l):0 for state in self.states for k,v in ETYPE.items() for year in years for l in ["linked","unlinked"]}
        self.patients = 0
        self.linked_visits = 0
        self.unlinked_visits = 0
        self.level_db_raw = None
        self.level_db_raw_snapshot = None
        self.level_db_codes = None
        self.level_db_codes_snapshot = None
        self.index_cache = None
        self.patients_cache = {}
        self.code_index_index = defaultdict(list)
        self.level_db_connected = False
        self.json_confg = json_config
        self.aggregate_visits = self.json_confg['ANALYTICS']['aggregate_visits']
        self.aggregate_readmits = self.json_confg['ANALYTICS']['aggregate_readmits']
        self.aggregate_revisits = self.json_confg['ANALYTICS']['aggregate_revisits']
        self.aggregate_patients = self.json_confg['ANALYTICS']['aggregate_patients']
        self.codes = None
        try:
            self.load()
        except:
            logging.exception("Could not load!")
            pass

    def fsync(self):
        if not os.path.isdir(self.base_dir):
            raise ValueError,"{} not a valid directory".format(self.base_dir)

    def load(self):
        for line in file(self.base_dir + "/AUDIT"):
            entries = line.strip().split("\t")
            if len(entries) == 3:
                state, t, count = entries
                if t == "patients" and state == "ALL":
                    self.patients = int(count)
                elif t == "linked_visits" and state == "ALL":
                    self.linked_visits = int(count)
                elif t == "unlinked_visits" and state == "ALL":
                    self.unlinked_visits = int(count)
                else:
                    self.state_counts[state][t] = int(count)
            elif len(entries) == 4:
                state, vtype, t, count = entries
                if vtype == "type_unlinked_visits":
                    self.type_counts[(state, int(t), "unlinked")] = int(count)
                if vtype == "type_linked_visits":
                    self.type_counts[(state, int(t), "linked")] = int(count)
            elif len(entries) == 5:
                state, vtype, t, year, count = entries
                if vtype == "year_type_unlinked_visits":
                    self.year_counts[(state, int(t), year, "unlinked")] = int(count)
                if vtype == "year_type_linked_visits":
                    self.year_counts[(state, int(t), year, "linked")] = int(count)
        for keyfile in glob.glob(self.code_index_dir+"/*.keys"):
            for line in file(keyfile):
                code_type,code,i,visit_count = line.strip().split('\t')
                self.code_index_index[(code_type,code)].append((i,visit_count))

    def get_patient(self,patient_key):
        if not self.level_db_connected:
            self.load_db()
        raw_string = self.level_db_raw_snapshot.get(patient_key.encode("utf-8"))
        p = Patient()
        p.ParseFromString(raw_string)
        return patient_key,p

    def iter_get_patients_by_keys(self,patient_keys):
        if not self.level_db_connected:
            self.load_db()
        for patient_key in patient_keys:
            if patient_key.encode("utf-8") in self.patients_cache:
                yield patient_key,self.patients_cache[patient_key]
            else:
                raw_string = self.level_db_raw_snapshot.get(patient_key.encode("utf-8"))
                p = Patient()
                p.ParseFromString(raw_string)
                self.patients_cache[patient_key] = p
                yield patient_key,p
        self.close_db()

    def iter_get_visits_by_keys(self,visit_keys):
        if not self.level_db_connected:
            self.load_db()
        for patient_key,visit_key in visit_keys:
            raw_string = self.level_db_raw_snapshot.get(patient_key.encode("utf-8"))
            p = Patient()
            p.ParseFromString(raw_string)
            for v in p.visits:
                if v.key == visit_key:
                    yield visit_key,v

    def iter_patients(self):
        if not self.level_db_connected:
            self.load_db()
        for patient_key, v in self.level_db_raw_snapshot:
            p = Patient()
            p.ParseFromString(v)
            yield patient_key,p

    def get_patient_keys_by_code(self,code,code_type=None):
        if code_type is None:
            code_type = extractor.get_code_type(code)
        if not self.level_db_connected:
            self.load_db()
        visit_count = 0
        patient_keys = set()
        visit_keys = []
        for i,vc in self.code_index_index[(code_type, code)]:
            visit_count += int(vc)
            q = "{}\t{}\t{}".format(code_type,code,i)
            lst = self.level_db_codes_snapshot.get(q)
            if lst is None:
                raise KeyError,q
            for line in lst.splitlines():
                pkey, vkey, linked = line.strip().split('\t')
                patient_keys.add(pkey)
                visit_keys.append((pkey,vkey))
        if visit_count != len(visit_keys):
            raise AssertionError,"{} != {} for {} {}".format(visit_count,len(visit_keys),code_type,code)
        return patient_keys,set(visit_keys)

    def get_patient_keys_by_primary_diagnosis(self,code):
        return self.get_patient_keys_by_code(code,'pdx')

    def load_codes(self):
        self.codes = {}
        for state,year,linked,vtype,ctype,code,count in self.iter_code_counts():
            if code not in self.codes:
                self.codes[code] = Code(code, self)
            self.codes[code].add_count(state,year,linked,vtype,ctype,code,count)

    def get_code(self,code):
        if self.codes is None:
            self.load_codes()
        return self.codes[code]

    def iter_codes(self):
        if self.codes is None:
            self.load_codes()
        for code in self.codes:
            yield self.codes[code]

    def iter_inpatient_procedure_codes(self):
        for c in self.iter_codes():
            if c.startswith('P'):
                yield c

    def iter_diagnosis_codes(self):
        for c in self.iter_codes():
            if c.startswith('D') and (not c.startswith('DG')):
                yield c

    def iter_outpatient_procedure_codes(self):
        for c in self.iter_codes():
            if c.startswith('C'):
                yield c

    def iter_code_counts(self):
        for line in file(self.base_dir+"/counts.txt"):
            entries = line.strip().split("\t")
            state,year,plinked,vtype,ctype,code,count= entries
            if len(code.strip()) > 1:
                count = int(count)
                linked = True if int(plinked) == 1 else False
                year = int(year)
                vtype = int(vtype)
                yield state,year,linked,vtype,ctype,code,count

    def load_index_cache(self):
        self.index_cache = {}
        for fname in glob.glob(self.base_dir+'/INDEX/*.metadata'):
            for line in file(fname):
                entries = line.strip().split('\t')
                self.index_cache[entries[1]] = self.base_dir+'/INDEX/'+entries[-2].split('/')[-1]

    def iter_patients_by_code(self,code):
        if self.index_cache is None:
            self.load_index_cache()
        if code in self.index_cache:
            logging.info("using index cache {}".format(code))
            fh = gzip.open(self.index_cache[code]+'.gz')
            stream_obj = stream_pb.Stream(fh)
            for i, s in enumerate(stream_obj.get_messages()):
                p = Patient()
                p.ParseFromString(s)
                yield p.patient_key, p
            stream_obj.close()
        else:
            logging.info("using raw db for {}".format(code))
            patient_keys, visit_keys = self.get_patient_keys_by_code(code)
            for pkey, p in self.iter_get_patients_by_keys(patient_keys):
                yield p.patient_key, p

    def load_db(self):
        self.level_db_raw = plyvel.DB(self.db, create_if_missing=False)
        self.level_db_raw_snapshot = self.level_db_raw.snapshot()
        self.level_db_codes = plyvel.DB(self.code_index_db, create_if_missing=False)
        self.level_db_codes_snapshot = self.level_db_codes.snapshot()
        self.level_db_connected = True

    def close_db(self):
        if self.level_db_connected:
            self.level_db_raw.close()
            self.level_db_codes.close()
        self.level_db_raw_snapshot = None
        self.level_db_codes_snapshot = None
        self.level_db_connected = False

    def merge(self):
        if self.identifier == 'HCUPCA':
            self.merge_hcupca()

    def prepare(self,test):
        self.fs_check()
        workers = Pool()
        db = plyvel.DB(self.db, create_if_missing=True)
        if self.identifier == 'HCUPCA':
            self.prepare_hcupca(test,db,workers)
        elif self.identifier == 'HCUPSID':
            self.prepare_hcupsid(test,db,workers)
        elif self.identifier == 'HCUPNRD':
            self.prepare_hcupnrd(test,db,workers)
        elif self.identifier == 'TX':
            self.prepare_tx(test,db,workers)
        else:
            raise NotImplementedError,"{}".format(self.identifier)
        db.close()

    def prepare_hcupsid(self,test,db,workers):
        i = 0
        for fname in glob.glob(self.raw_dir + "*.gz"):
            start = time.time()
            unlinked = False
            if "unlinked" in fname:
                unlinked = True
            state = fname.split("/")[-1].split('.')[-2]
            fh = gzip.open(fname)
            logging.info("starting {} with {} and {}".format(fname, state, unlinked))
            line_buffer = []
            for i, line in enumerate(fh):
                line_buffer.append(line)
                if len(line_buffer) == 10000:
                    res = workers.map(formats.hcupsid.process_buffer_hcup, line_buffer)
                    formats.hcupsid.finalize_hcup(res, unlinked, state, db)
                    line_buffer = []
                if (i + 1) % 10 ** 5 == 0:
                    end = time.time()
                    logging.info("processing {} with {} in {} seconds".format(fname, i, round(end - start, 2)))
                    start = time.time()
            res = workers.map(formats.hcupsid.process_buffer_hcup, line_buffer)
            formats.hcupsid.finalize_hcup(res, unlinked, state, db)
            logging.info("finished {} with {}".format(fname, i))

    def prepare_hcupca(self,test,db,workers):
        i = 0
        for fname in glob.glob(self.raw_dir + "*.gz"):
            start = time.time()
            unlinked = False
            if "unlinked" in fname:
                unlinked = True
            state = fname.split("/")[-1].split('.')[-2]
            fh = gzip.open(fname)
            logging.info("starting {} with {} and {}".format(fname, state, unlinked))
            line_buffer = []
            for i, line in enumerate(fh):
                line_buffer.append(line)
                if len(line_buffer) == 2000:
                    res = workers.map(formats.hcupca.process_buffer_hcup, line_buffer)
                    formats.hcupca.finalize_hcup(res, unlinked, state, db)
                    line_buffer = []
                if (i + 1) % 10 ** 4 == 0:
                    end = time.time()
                    logging.info("processing {} {} with {} in {} seconds".format(state,fname, i, round(end - start, 2)))
                    start = time.time()
            res = workers.map(formats.hcupca.process_buffer_hcup, line_buffer)
            formats.hcupca.finalize_hcup(res, unlinked, state, db)
            logging.info("finished {} with {}".format(fname, i))

    def prepare_hcupnrd(self, test,db,workers):
        i = 0
        self.nrd_setup()
        start = time.time()
        logging.info("starting HCUP NRD with Single")
        line_buffer = []
        for i, line in enumerate(file(self.raw_dir + "single.txt")):
            line_buffer.append(line)
            if len(line_buffer) == 20000:
                res = workers.map(formats.hcupnrd.process_buffer_hcupnrd, line_buffer)
                formats.hcupnrd.finalize_hcupnrd(res, db)
                line_buffer = []
                end = time.time()
                logging.info("processing  {} from single.txt in {} seconds".format(i + 1, round(end - start, 2)))
                start = time.time()
        res = workers.map(formats.hcupnrd.process_buffer_hcupnrd, line_buffer)
        formats.hcupnrd.finalize_hcupnrd(res, db)
        logging.info("finished HCUPNRD with Multiple")
        patient_line = ""
        last_patient_key = None
        line_buffer = []
        for i, line in enumerate(file(self.raw_dir + "multiple_sorted.txt")):
            current_patient_key, current_line = line.strip("\r\n").split("\t")
            if last_patient_key is None:
                patient_line = current_patient_key + "\t" + current_line
                last_patient_key = current_patient_key
            elif current_patient_key == last_patient_key:
                patient_line += "\t" + current_line
            elif current_patient_key != last_patient_key:
                line_buffer.append(patient_line)
                patient_line = current_patient_key + "\t" + current_line
                last_patient_key = current_patient_key
                if len(line_buffer) == 20000:
                    res = workers.map(formats.hcupnrd.process_buffer_hcupnrd, line_buffer)
                    formats.hcupnrd.finalize_hcupnrd(res, db)
                    line_buffer = []
                    end = time.time()
                    logging.info("{} patients with {} visits {} seconds".format(20000, i,round(end - start,2)))
                    start = time.time()
        line_buffer.append(patient_line)
        res = workers.map(formats.hcupnrd.process_buffer_hcupnrd, line_buffer)
        formats.hcupnrd.finalize_hcupnrd(res, db)
        logging.info("finished HCUPNRD with multiple")

    def prepare_tx(self,test,db,workers):
        i = 0
        start = time.time()
        files = ["{}/{}".format(self.raw_dir,fname) for fname in self.json_confg['RAW_FILES']]
        for fname in files:
            quarter, year = fname.split("PUDF_base")[1].split('_')[0].split('q')
            logging.info((quarter, year))
            fh = gzip.open(fname)
            line_buffer = []
            for i, line in enumerate(fh):
                if i > 0:
                    line = "\t".join([quarter, year, line])
                    line_buffer.append(line)
                    if len(line_buffer) == 40000:
                        res = workers.map(formats.texas.process_buffer_texas, line_buffer)
                        formats.texas.finalize_texas(res, db)
                        line_buffer = []
                        if test:
                            break
                    if (i + 1) % 10 ** 5 == 0:
                        end = time.time()
                        logging.info("processing {} with {} in {} seconds".format(fname, i, round(end - start, 2)))
                        start = time.time()
            res = workers.map(formats.texas.process_buffer_texas, line_buffer)
            formats.texas.finalize_texas(res, db)
            logging.info("finished {} with {}".format(fname, i))

    def nrd_setup(self):
        """
        Sort NRD Dataset
         14325172 14325172 5493981867 NRD_2013_Core.CSV
         6018285 18054855 2441276066 multiple.txt
         6018285 18054855 2441276066 multiple_sorted.txt
         8306887 8306887 3130826745 single.txt
         34668629 58741769 13507360744 total
        :return: Generates single.txt and multiple_sorted.txt
        """
        if self.identifier == 'HCUPNRD':
            if not (os.path.isfile(self.raw_dir+"multiple_sorted.txt") and os.path.isfile(self.raw_dir+"single.txt")):
                patient_visit_counter = defaultdict(int)
                for year,fname in formats.hcupnrd.FILES:
                    parser = formats.hcupnrd.PARSERS[year]
                    vlink_index = parser['NRD_VisitLink']
                    for i,line in enumerate(file(self.raw_dir+fname)):
                        entries = line.strip('\r\n').split(',')
                        patient_visit_counter[entries[vlink_index]] += 1
                logging.info(len(patient_visit_counter))
                logging.info(len([k for k in patient_visit_counter.itervalues() if k == 1]))
                single = open(self.raw_dir+"single.txt",'w')
                multiple = open(self.raw_dir+"multiple.txt",'w')
                single_count,multiple_count = 0,0
                for year,fname in formats.hcupnrd.FILES:
                    parser = formats.hcupnrd.PARSERS[year]
                    vlink_index = parser['NRD_VisitLink']
                    daystoevent_index = parser['NRD_DaysToEvent']
                    for i,line in enumerate(file(self.raw_dir+fname)):
                        entries = line.strip('\r\n').split(',')
                        visit_link = entries[vlink_index]
                        daystoevent = entries[daystoevent_index]
                        if patient_visit_counter[visit_link] == 1:
                            single.write("{}\t{}|SEP|{}|SEP|{}".format(visit_link,daystoevent,year,line))
                            single_count += 1
                        else:
                            multiple.write("{}\t{}|SEP|{}|SEP|{}".format(visit_link,daystoevent,year,line))
                            multiple_count += 1
                single.close()
                multiple.close()
                command = "sort {} > {}".format(self.raw_dir+"multiple.txt",self.raw_dir+"multiple_sorted.txt")
                logging.info("executing {}".format(command))
                os.system(command)
                logging.info((single_count,multiple_count))
            else:
                logging.warning("found pre parsed single and muliple_sorted, skipping this step")

    def merge_hcupca(self):
        """
        Merge HCUP files from multiple years in to a single file for each state
        """
        for state in self.states:
            combiner = formats.hcupca.merge.Combiner(state,self.asc_dir,self.raw_dir)
            logging.info("started {} at {}".format(state,self.raw_dir))
            for f in formats.hcupca.merge.FILES:
                if f['state'] == state:
                    combiner.add_file(f)
            logging.info("Finished {}".format(self.raw_dir))
            combiner.combine()

    def audit(self):
        args = ['./chlib/cppcode/bin/Debug/cpp', 'VERIFY', self.identifier]
        logging.info("starting audit")
        logging.info(args)
        p = subprocess.Popen(args=args)
        p.wait()
        if p.returncode != 0:
            raise ValueError,"Could not audit the dataset"
        return p.returncode

    def index_codes(self):
        fnames = [fname for fname in glob.glob(self.code_index_dir + "/*.gz")]
        for fname in fnames:
            logging.info("indexing {}".format(fname))
            i = fname.split('/')[-1].replace("index_", "").split(".")[0]
            tempfname = fname.replace(".gz", ".temp")
            logging.info("expanding file {} {}".format(fname, tempfname))
            if sys.platform == 'darwin':
                os.system("gzcat -d {} > {}".format(fname, tempfname))
            else:
                os.system("zcat -d {} > {}".format(fname, tempfname))
            logging.info("expanded")
            command = "./chlib/cppcode/bin/Debug/cpp CINDEX {} {} {}".format(self.identifier, tempfname, i)
            logging.info(command)
            rc = os.system(command)
            logging.info(rc)
            os.system("rm {}".format(tempfname))
            logging.info("finishing {} with return code {}".format(fname,rc))

    def setup(self,skip_prepare,skip_audit,skip_code_index,test=False):
        if not skip_prepare:
            self.prepare(test)
        if not skip_audit:
            self.audit()
        if not skip_code_index:
            self.index_codes()

    def fs_check(self):
        if os.path.isdir(self.base_dir):
            for directory in [self.result_dir, self.code_index_dir, self.N4]:
                if not os.path.isdir(directory):
                    try:
                        os.makedirs(directory)
                    except OSError:
                        logging.exception("Could not create directory {}".format(directory))
        else:
            logging.warning(
                "Root Directory {} does not exists and thus no sub directories were created".format(self.base_dir))

    def process_code(self,code):
        E = extractor.Extractor([code, ], self)
        E.extract()
        if len(self.years) > 1:
            max_year = max(self.years)
        else:
            max_year = max(self.years)+1
        # if self.aggregate_visits:
        #     aggregate_visits.aggregate_events(code, E.get_inpatient_visits(code), self.identifier, self.result_dir,reduce_mode_mini=False)
        # if self.aggregate_readmits:
        #     unlinked_nodes, nodes, edges = E.get_readmit_nodes_edges(code)
        #     aggregate_edges.readmits.aggregate_readmits(code, unlinked_nodes, nodes, edges, self.identifier, self.result_dir,max_year)
        # if self.aggregate_revisits:
        #     subsets = E.get_revisit_nodes_edges(code)
        #     for k, v in subsets.iteritems():
        #         aggregate_edges.revisits.aggregate_revisits(k, v['unlinked_nodes'], v['nodes'], v['edges'], self.identifier, self.result_dir, max_year)
        # if self.aggregate_patients:
        #     aggregate_patients.aggregate_patients(code,E.get_patients(code),self.identifier,self.result_dir,self.patients)
        raise NotImplementedError

    def pre_compute(self):
        """
        Pre computes for faster retrieval
        :return:
        """
        codes = set()
        for c in self.iter_codes():
            if c.visits_count() > 100:
                codes.add(c.code)
        with open("{}/codes_hashes.txt".format(self.base_dir), 'w') as f:
            codeset = {code: hash(code) % 20 for code in codes if code != 'DG-1'}
            f.write('\n'.join(["{}\t{}".format(k, v) for k, v in codeset.iteritems()]) + '\n')
        for k in range(0,20):
            args = ['./chlib/cppcode/bin/Debug/cpp', 'INDEX', self.identifier,str(k)]
            try:
                os.mkdir('{}/INDEX/'.format(self.base_dir))
            except:
                pass
            logging.info("starting pre compute")
            logging.info(args)
            p = subprocess.Popen(args=args)
            p.wait()
            if p.returncode != 0:
                raise ValueError,"Could pre compute dataset"
            files = [fname for fname in glob.glob('{}/INDEX/*.n4'.format(self.base_dir))]
            pool = Pool()
            pool.map(compress,files)
            pool.close()
            logging.info("finished precomputing {} with {}".format(k,p.returncode))


def compress(fname):
    p = subprocess.Popen(args=['gzip', fname])
    p.wait()
    logging.info("gzipped {}".format(fname))
    return p.returncode