//
// Created by Akshay on 2/16/16.
//

#ifndef ANALYSIS_AUDITOR_H
#define ANALYSIS_AUDITOR_H

#include "../protos/pvisit.pb.h"
#include <iostream>

#include <unordered_map>
#include <unordered_set>
#include "leveldb/db.h"
#include <fstream>
#include <cstdlib>
namespace audit
{


class Auditor
{
public:
    std::unordered_map<std::string,int> counter;
    void add(comphealth::Patient* p);
    void finalize(std::ofstream* output,std::string prefix);
    ~Auditor(){
        counter.clear();
    }
};

class CodeAuditor
{
public:
    int index;
    std::string path,N4_path,code_path;
    std::ofstream *index_file, *N4_output, *count_output;
    std::unordered_map<std::string,int> counter,N4_counter;
    std::unordered_set<std::string> all_dx;
    CodeAuditor(std::string _code_path,std::string _path,std::string _N4_path);
    void pause();
    void finish();
    void add(std::string key,comphealth::Patient* p);
    ~CodeAuditor(){
        counter.clear();
    }
};

}
#endif //ANALYSIS_AUDITOR_H
