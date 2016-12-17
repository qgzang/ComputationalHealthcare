#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <unordered_set>
#include "pystring/pystring.h"
#include "protos/penums.pb.h"
#include "protos/pvisit.pb.h"
#include "indexer/indexer.h"
#include <glog/logging.h>
#include "leveldb/db.h"
#include "cindexer/cindexer.h"
//#include "load/loader.h"
#include "json11/json11.hpp"
#include <thread>
#include "verify/verify.h"


json11::Json parse_config(string fpath){
    ifstream fconfig(fpath);
    string buf,line;
    while (std::getline(fconfig, line)) {
        buf += line;
    }
    string err;
    json11::Json config = json11::Json::parse(buf, err);
    if (!err.empty()){
        printf("Failed: %s\n", err.c_str());
        throw "Error";
    }
    return config;
}

const string CONFIG_PATH = "config.json";


void filter_codes_hash(string hash_code,std::unordered_map<std::string,std::string> &codes_hash, json11::Json config,std::string dataset)
{
    string input_path = config["DATASETS"][dataset]["ROOT"].string_value()+"/codes_hashes.txt";
    ifstream input(input_path);
    string line;
    vector<string>entries;
    while(getline(input,line)){
        line = pystring::strip(line);
        pystring::split(line,entries,"\t");
        codes_hash[entries[0]] = entries[1];
    }
    vector<string> deletes;
    for (auto k:codes_hash){
        if (k.second != hash_code){
            deletes.push_back(k.first);
        }
    }
    for(auto k:deletes){
        codes_hash.erase(k);
    }
}


int main(int argc, char* argv[])
{
    std::string hash_code;
    std::string instant_path,cindex_path,file_index,instant_keys_path;
    std::unordered_set<string> codes;
    string option = argv[1];
    string dataset = argv[2];
    auto config = parse_config(CONFIG_PATH);
    google::InitGoogleLogging(argv[0]);
    google::SetLogDestination(google::INFO,config["cpp_log_path"].string_value().c_str());
    LOG(INFO) << "type " << argv[1];
    if (option == "INDEX"){
        hash_code = argv[3];
        LOG(INFO) << "hash code " << argv[3];
        std::unordered_map<std::string,std::string> codes_hash;
        filter_codes_hash(hash_code,codes_hash,config,dataset);
        LOG(INFO) << "\n codes_hash size "<< codes_hash.size();
        indexer::Indexer code_indexer(codes_hash,config,dataset);
        code_indexer.index_DB();
        code_indexer.close_indexer();
    }
    else if (option == "CINDEX"){
        cindex_path = argv[3];
        file_index = argv[4];
        LOG(INFO) << " processing file:" << cindex_path;
        LOG(INFO) << " with index:" << file_index;
        cindexer::index_codes(cindex_path,file_index,config,dataset);
    }
    else if(option == "VERIFY")
    {
        verify::code_verify(config,dataset);
    }
    else if(option == "QUICK")
    {
        verify::quick_verify(config,dataset);
    }
    else
    {
        LOG(ERROR) << "ERROR NO OPTION SPECIFIED!!!";
    }
    return 0;
}

