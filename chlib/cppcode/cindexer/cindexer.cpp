//
// Created by Akshay on 5/15/16.
//

#include "cindexer.h"

void cindexer::index_codes(std::string _filename,std::string _i,json11::Json _config,std::string _dataset){
    google::FlushLogFiles(google::INFO);
    leveldb::DB* db;
    std::vector<std::string> entries;
    std::unordered_map<std::string,std::string> data;
    std::unordered_map<std::string,int> counter;
    leveldb::Options options;
    leveldb::WriteOptions woptions;
    std::ifstream input(_filename);
    std::ofstream output(pystring::replace(_filename,".txt.temp",".keys"));
    std::string line;
    const std::string dbpath = _config["DATASETS"][_dataset]["ROOT"].string_value()+"/CINDEXDB";
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options,dbpath, &db);
    if (!status.ok())
    {
        std::cerr << "Unable to open/create test database "<< dbpath << std::endl;
        std::cerr << status.ToString() << std::endl;
    }
    LOG(INFO) << "Connected to database" << std::endl;
    LOG(INFO) << "Starting with file " << _filename <<std::endl;
    google::FlushLogFiles(google::INFO);
    while (std::getline(input,line)){
        entries.clear();
        pystring::split(pystring::strip(line),entries,"\t");
        data[entries[0]+"\t"+entries[1]+"\t"+_i] += entries[2]+"\t"+entries[3]+"\t"+entries[4]+"\n";
        counter[entries[0]+"\t"+entries[1]+"\t"+_i]++;
    }
    LOG(INFO) << "Finished Reading" << std::endl;
    google::FlushLogFiles(google::INFO);
    leveldb::WriteBatch batch;
    for(auto k:data){
        output << k.first << "\t" <<counter[k.first] << "\n";
        batch.Put(k.first,k.second);
    }
    db->Write(woptions,&batch);
    output.close();
    delete db;
    LOG(INFO) << "Finished adding to database" << std::endl;
    google::FlushLogFiles(google::INFO);
}
