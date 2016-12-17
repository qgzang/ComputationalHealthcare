//
// Created by Akshay on 2/16/16.
//

#include "auditor.h"


void audit::Auditor::finalize(std::ofstream* output,std::string prefix){
    std::vector<std::string> d;
    for (auto k:counter){
        if(k.second <= 20){
            d.push_back(k.first);
        }
    }
    for (auto k:d){
        counter.erase(k);
    }
    for (auto k: counter){
        if (k.second > 0){
            *output << prefix << "\t" << k.first << "\t" << k.second << "\n";
        }
    }
}

void audit::Auditor::add(comphealth::Patient* p)
{
    if (p->linked()){
        counter["patients"]++;
        for (auto v:p->visits()){
            counter["linked_visits"]++;
            counter["type_linked_visits\t"+std::to_string(v.vtype())]++;
            counter["year_type_linked_visits\t"+std::to_string(v.vtype())+"\t"+std::to_string(v.year())]++;
        }
    }
    else{
        counter["unlinked_visits"]++;
        counter["type_unlinked_visits\t"+std::to_string(p->visits(0).vtype())]++;
        counter["year_type_unlinked_visits\t"+std::to_string(p->visits(0).vtype())+"\t"+std::to_string(p->visits(0).year())]++;
    }
}

audit::CodeAuditor::CodeAuditor(std::string _code_path,std::string _path,std::string _N4_path)
{
    code_path = _code_path;
    path = _path;
    N4_path = _N4_path;
    index = 1;
    index_file = new std::ofstream(path+"/index_"+std::to_string(index)+".txt",std::ios::out);
    N4_output = new std::ofstream(N4_path+"/multicount.txt",std::ios::out);
}


void audit::CodeAuditor::add(std::string key,comphealth::Patient* p)
{
    all_dx.clear();
    for (auto v:p->visits())
    {
        for (auto dx:v.dxs())
        {
            counter["ALL\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tdx\t"+dx]++;
            counter[v.state()+"\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tdx\t"+dx]++;
            *index_file << "dx" << "\t" << dx << "\t" << key << "\t" << v.key() << "\t" << (p->linked()? 1 :0) << "\n";
            all_dx.insert(dx);
        }
        for (auto ex:v.exs()){
            counter["ALL\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tex\t"+ex]++;
            counter[v.state()+"\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tex\t"+ex]++;
            *index_file << "ex" << "\t" << ex << "\t" << key << "\t" << v.key() << "\t" << (p->linked()? 1 :0) << "\n";
            all_dx.insert(ex);
        }
        for (auto pr:v.prs()) {
            if (pr.pcode().length() > 2)
            {
                counter["ALL\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tpr\t"+pr.pcode()]++;
                counter[v.state()+"\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tpr\t"+pr.pcode()]++;
                *index_file << "pr" << "\t" << pr.pcode() << "\t" << key << "\t" << v.key() << "\t" << (p->linked()? 1 :0) << "\n";
            }
        }
        if (v.drg() != "DG-1"){
            counter["ALL\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tdrg\t"+v.drg()]++;
            counter[v.state()+"\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tdrg\t"+v.drg()]++;
            *index_file << "drg" << "\t" << v.drg() << "\t" << key << "\t" << v.key() << "\t" << (p->linked()? 1 :0) << "\n";
        }
        counter["ALL\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tpdx\t"+v.primary_diagnosis()]++;
        counter[v.state()+"\t"+std::to_string(v.year())+"\t"+(p->linked()? "1" :"0")+"\t"+std::to_string(v.vtype())+"\tpdx\t"+v.primary_diagnosis()]++;
        *index_file << "pdx" << "\t" << v.primary_diagnosis() << "\t" << key << "\t" << v.key() << "\t" << (p->linked()? 1 :0) << "\n";
    }
    if (p->linked())
    {
        for(auto dx_first:all_dx)
        {
            for(std::string dx_second:all_dx)
            {
                if (dx_first >= dx_second){
                    N4_counter[dx_first+"\t"+dx_second]++;
                }
            }
        }
    }
}

void audit::CodeAuditor::pause() {
    for (auto k: N4_counter){
        *N4_output << k.first << "\t" << k.second << "\n";
    }
    N4_output->flush();
    N4_counter.clear();
    index_file->close();
    delete index_file;
    index++;
    std::string command = "gzip "+path+"/index_*.txt";
    std::system(command.c_str());
    index_file = new std::ofstream(path+"/index_"+std::to_string(index)+".txt",std::ios::out);
}

void audit::CodeAuditor::finish(){
    for (auto k: N4_counter){
        *N4_output << k.first << "\t" << k.second << "\n";
    }
    N4_output->close();
    delete N4_output;
    N4_counter.clear();
    count_output = new std::ofstream(code_path+"/counts.txt",std::ios::out);
    for (auto k: counter){
        *count_output << k.first << "\t" << k.second << "\n";
    }
    count_output->close();
    delete count_output;
    counter.clear();
    index_file->close();
    delete index_file;
    index++;
    std::string command = "gzip "+path+"/index_*.txt";
    std::system(command.c_str());
}

