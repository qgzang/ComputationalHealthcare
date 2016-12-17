//
// Created by Akshay on 2/15/16.
//

#include "verify.h"

void verify::code_verify(json11::Json config,std::string dataset){
    leveldb::DB* db;
    leveldb::Options options;
    double elapsed_secs;
    clock_t begin,end,initial;
    comphealth::Patient *p;
    std::map<std::string,audit::Auditor> state_audit;
    std::unordered_map<std::string,int>* audit_results;
    audit::Auditor vaudit;
    audit::CodeAuditor audit(config["DATASETS"][dataset]["ROOT"].string_value(),config["DATASETS"][dataset]["ROOT"].string_value()+"/CINDEX",config["DATASETS"][dataset]["ROOT"].string_value()+"/N4");
    const std::string dbpath = config["DATASETS"][dataset]["ROOT"].string_value()+"/DB";
    leveldb::Status status = leveldb::DB::Open(options,dbpath, &db);
    if (!status.ok())
    {
        std::cerr << "Unable to open/create test database "<< dbpath << std::endl;
        std::cerr << status.ToString() << std::endl;
    }
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    int count = 0;
    p = new comphealth::Patient();
    begin = clock();
    initial = clock();
    for (it->SeekToFirst(); it->Valid(); it->Next()){
        if (pystring::startswith(it->key().ToString(),"0"))
        {
            break;
        }
        p->ParseFromString(it->value().ToString());
        vaudit.add(p);
        state_audit[p->visits(0).state()].add(p);
        audit.add(it->key().ToString(),p);
        p->Clear();
        count++;
        if (count % 1000000 == 0){
            end = clock();
            elapsed_secs = (double)(end - begin) / CLOCKS_PER_SEC;
            begin = clock();
            std::cout << count << "\t" << elapsed_secs << "\n";
            audit.pause();
        }
    }
    audit.finish();
    std::ofstream output(config["DATASETS"][dataset]["ROOT"].string_value()+"/AUDIT");
    vaudit.finalize(&output,"ALL");
    for (auto a: state_audit) {
        a.second.finalize(&output,a.first);
    }
    output.close();
    end = clock();
    std::cout << count << "\t"  << (double)(end - initial) / CLOCKS_PER_SEC <<  std::endl;
    if (!it->status().ok())
    {
        std::cerr << "An error was found during the scan" << std::endl;
        std::cerr << it->status().ToString() << std::endl;
    }
    delete it;
    delete db;
};


void verify::quick_verify(json11::Json config,std::string dataset){
    leveldb::DB* db;
    leveldb::Options options;
    double elapsed_secs;
    clock_t begin,end,initial;
    comphealth::Patient *p;
    const std::string dbpath = config["DATASETS"][dataset]["ROOT"].string_value()+"/DB";
    leveldb::Status status = leveldb::DB::Open(options,dbpath, &db);
    if (!status.ok())
    {
        std::cerr << "Unable to open/create test database "<< dbpath << std::endl;
        std::cerr << status.ToString() << std::endl;
    }
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    int count = 0;
    int zcount = 0;
    p = new comphealth::Patient();
    begin = clock();
    initial = clock();
    for (it->SeekToFirst(); it->Valid(); it->Next()){
        if (pystring::startswith(it->key().ToString(),"0"))
        {
            zcount++;
        }
        p->ParseFromString(it->value().ToString());
        p->Clear();
        count++;
        if (count % 1000000 == 0){
            end = clock();
            elapsed_secs = (double)(end - begin) / CLOCKS_PER_SEC;
            begin = clock();
            std::cout << count << "\t" << zcount << "\t" << elapsed_secs << "\n";
        }
    }
    end = clock();
    std::cout << count << "\t" << zcount << "\t"  << (double)(end - initial) / CLOCKS_PER_SEC <<  std::endl;
    if (!it->status().ok())
    {
        std::cerr << "An error was found during the scan" << std::endl;
        std::cerr << it->status().ToString() << std::endl;
    }
    delete it;
    delete db;
};

