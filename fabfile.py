import os,sys,logging,shutil,random,time,boto3,glob,gzip,json
from collections import defaultdict
import chlib
import django,plyvel
from fabric.api import env,local,run,sudo,put,cd,lcd,puts,task


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='logs/fab.log',
                    filemode='a')


@task
def compile_protocols():
    protocols = ["penums.proto","pvisit.proto","pstat.proto","pn1.proto","pn2.proto","pn3.proto","pn4.proto"]
    with lcd('chlib/entity/protocols'):
        for fname in protocols:
            local('protoc --python_out=../ --cpp_out=../../cppcode/protos/ {}'.format(fname))




@task
def clear_logs():
    """
    remove logs
    """
    local('rm logs/*.log &')
    local('rm logs/cpp* &')







@task
def process_code(code):
    datasets = chlib.data.Data.load_from_config('config.json')
    for k,d in datasets.iteritems():
        logging.info("started {}".format(d.identifier))
        try:
            d.process_code(code)
        except plyvel._plyvel.Error:
            pass
            logging.info("could not open database for {}".format(d.identifier))
        else:
            d.close_db()
            logging.info("finished {}".format(d.identifier))


@task
def prepare_tx(skip_prepare=False):
    TX = chlib.data.Data.get_from_config('config.json','TX')
    TX.setup(skip_prepare,False,False,test=False)


@task
def prepare_nrd(skip_prepare=False):
    HCUPNRD = chlib.data.Data.get_from_config('config.json','HCUPNRD')
    HCUPNRD.setup(skip_prepare,False,False,test=False)

@task
def precompute(dataset):
    D = chlib.data.Data.get_from_config('config.json',dataset)
    D.pre_compute()


def process_code_dataset(code_dataset_id):
    code,dataset_id = code_dataset_id
    logging.info("started {}".format(code))
    dataset = chlib.data.Data.get_from_config('config.json',dataset_id)
    dataset.process_code(code)
    logging.info("finished {}".format(code))


@task
def process_all(dataset_id):
    import multiprocessing
    dataset = chlib.data.Data.get_from_config('config.json', dataset_id)
    pool = multiprocessing.Pool()
    codes = [(c.code, dataset_id) for c in dataset.iter_codes() if c.visits_count() > 100]
    logging.info("starting {} codes for {} ".format(len(codes), dataset_id))
    pool.map(process_code_dataset, codes)
    pool.close()
    logging.info("finished {} codes for {} ".format(len(codes), dataset_id))

@task
def compile_cpp_code():
    with lcd("chlib/cppcode/"):
        local("cmake .")
        local("make")
        try:
            local("mkdir -p bin/Debug/")
        except:
            pass
        local("mv cpp bin/Debug/")
