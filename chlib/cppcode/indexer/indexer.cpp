//
// Created by Akshay on 8/20/15.
//

#include "indexer.h"
//
// Created by Akshay on 6/3/15.
//


using namespace google::protobuf::io;

void process_n1_visit(comphealth::Visit& v,std::unordered_map<std::string,std::string>& codes,unordered_map<string,CodedOutputStream*>& output_files,unordered_map<string,int>& output_counts)
{
    string temps;
    if (v.vtype() == comphealth::ETYPE::IP){
        for(auto prcode:v.prs()){
            if (codes.find(prcode.pcode()) != codes.end())
            {
                temps = v.SerializeAsString();
                output_files[prcode.pcode()]->WriteVarint32(temps.size());
                output_files[prcode.pcode()]->WriteString(temps);
                output_counts[prcode.pcode()]++;
            }
        }
        if (codes.find(v.primary_diagnosis()) != codes.end())
        {
            temps = v.SerializeAsString();
            output_files[v.primary_diagnosis()]->WriteVarint32(temps.size());
            output_files[v.primary_diagnosis()]->WriteString(temps);
            output_counts[v.primary_diagnosis()]++;

        }
        if (codes.find(v.drg()) != codes.end())
        {
            temps = v.SerializeAsString();
            output_files[v.drg()]->WriteVarint32(temps.size());
            output_files[v.drg()]->WriteString(temps);
            output_counts[v.drg()]++;

        }
    }

}

void process_n4_patients(comphealth::Patient* p,std::unordered_map<std::string,std::string>& codes,unordered_map<string,CodedOutputStream*>& output_files,unordered_map<string,int>& output_counts)
{
    string temps;
    unordered_set<string> selected;
    selected.clear();
    for(auto v:p->visits())
    {
        for (auto dcode:v.dxs()) {
            if (codes.find(dcode) != codes.end()) {
                selected.insert(dcode);
            }
        }
        for(auto prcode:v.prs()){
            if (codes.find(prcode.pcode()) != codes.end())
            {
                selected.insert(prcode.pcode());
            }
        }
        if (codes.find(v.drg()) != codes.end()){
            selected.insert(v.drg());
        }
    }
    for(auto d:selected){
        p->set_raw("");
        temps = p->SerializeAsString();
        output_files[d]->WriteVarint32(temps.size());
        output_files[d]->WriteString(temps);
        output_counts[d]++;
    }
}

void get_readmit_edges(comphealth::Patient *p, vector<comphealth::VisitEdge *> &edge_vector){
    /*
     */
    comphealth::VisitEdge* e;
    vector<int> inpatient;
    int index = 0;
    int vindex = 0;
    for(auto v:p->visits()){
        if (v.vtype() ==  comphealth::IP){
            inpatient.push_back(index);
        }
        index++;
    }
    if (inpatient.size() <= 1){
        return;
    }
    else{
        for(int i:inpatient)
        {
            if (vindex < inpatient.size()-1)
            {
                e = new comphealth::VisitEdge();
                e->mutable_initial()->CopyFrom(p->visits(i));
                e->mutable_sub()->CopyFrom(p->visits(inpatient[vindex+1]));
                edge_vector.push_back(e);
            }
            vindex++;
        }
//        cout<<edge_vector.size()<<"\t"<<inpatient.size()<<"\t"<<vindex<<"\t"<<p.visits_size()<<"\t"<<endl;
    }
}

void get_revisit_edges(comphealth::Patient *p, vector<comphealth::VisitEdge *> &edge_vector){
    /*
     */
    comphealth::VisitEdge* e;
    vector<int> inpatient;
    int index = 0;
    int vindex = 0;
    for(auto v:p->visits()){
        inpatient.push_back(index);
        index++;
    }
    if (inpatient.size() <= 1){
        return;
    }
    else{
        for(int i:inpatient)
        {
            if (vindex < inpatient.size()-1)
            {
                e = new comphealth::VisitEdge();
                e->mutable_initial()->CopyFrom(p->visits(i));
                e->mutable_sub()->CopyFrom(p->visits(inpatient[vindex+1]));
                edge_vector.push_back(e);
            }
            vindex++;
        }
//        cout<<edge_vector.size()<<"\t"<<inpatient.size()<<"\t"<<vindex<<"\t"<<p.visits_size()<<"\t"<<endl;
    }
}


void process_n2_nodes(comphealth::Patient* p,comphealth::Visit& v,std::unordered_map<std::string,std::string>& codes,unordered_map<string,CodedOutputStream*>& node_output_files,unordered_map<string,int>& node_output_counts,unordered_map<string,int>& unlinked_counter){
    string temps = v.SerializeAsString();
    if (v.vtype() == comphealth::ETYPE::IP){
        for(auto prcode:v.prs()){
            if (codes.find(prcode.pcode()) != codes.end())
            {
                if (p->linked())
                {
                    node_output_files[prcode.pcode()]->WriteVarint32(temps.size());
                    node_output_files[prcode.pcode()]->WriteString(temps);
                    node_output_counts[prcode.pcode()]++;
                }
                else{
                    unlinked_counter[prcode.pcode()]++;
                }
            }
        }
        if (codes.find(v.primary_diagnosis()) != codes.end())
        {
            if (p->linked())
            {
                temps = v.SerializeAsString();
                node_output_files[v.primary_diagnosis()]->WriteVarint32(temps.size());
                node_output_files[v.primary_diagnosis()]->WriteString(temps);
                node_output_counts[v.primary_diagnosis()]++;
            }
            else{
                unlinked_counter[v.primary_diagnosis()]++;
            }

        }
        if (codes.find(v.drg()) != codes.end())
        {
            if (p->linked())
            {
                temps = v.SerializeAsString();
                node_output_files[v.drg()]->WriteVarint32(temps.size());
                node_output_files[v.drg()]->WriteString(temps);
                node_output_counts[v.drg()]++;
            }
            else{
                unlinked_counter[v.drg()]++;
            }
        }
    }
}

void process_n2_edges(comphealth::Patient* p,std::unordered_map<std::string,std::string>& codes,unordered_map<string,CodedOutputStream*>& edge_output_files,unordered_map<string,int>& edge_output_counts){
    vector<comphealth::VisitEdge*> edge_vector;
    string temps;
    edge_vector.clear();
    get_readmit_edges(p,edge_vector);
    for (auto e:edge_vector){
        for(auto prcode:e->initial().prs()){
            if (codes.find(prcode.pcode()) != codes.end())
            {
                temps = e->SerializeAsString();
                edge_output_files[prcode.pcode()]->WriteVarint32(temps.size());
                edge_output_files[prcode.pcode()]->WriteString(temps);
                edge_output_counts[prcode.pcode()]++;
            }
        }
        if (codes.find(e->initial().primary_diagnosis()) != codes.end()){
            temps = e->SerializeAsString();
            edge_output_files[e->initial().primary_diagnosis()]->WriteVarint32(temps.size());
            edge_output_files[e->initial().primary_diagnosis()]->WriteString(temps);
            edge_output_counts[e->initial().primary_diagnosis()]++;
        }
        if (codes.find(e->initial().drg()) != codes.end()){
            temps = e->SerializeAsString();
            edge_output_files[e->initial().drg()]->WriteVarint32(temps.size());
            edge_output_files[e->initial().drg()]->WriteString(temps);
            edge_output_counts[e->initial().drg()]++;
        }
        delete e;
    }
}

void process_n3_nodes(comphealth::Patient* p,comphealth::Visit& v,std::unordered_map<std::string,std::string>& codes,unordered_map<string,CodedOutputStream*>& node_output_files,unordered_map<string,int>& node_output_counts,unordered_map<string,int>& unlinked_counter){
    string temps, current_dx,current_pr;
    current_dx = v.primary_diagnosis();
    for(auto prcode:v.prs()){
        current_pr = prcode.pcode();
        if (codes.find(current_pr) != codes.end())
        {
            if (p->linked())
            {
                temps = v.SerializeAsString();
                node_output_files[current_pr]->WriteVarint32(temps.size());
                node_output_files[current_pr]->WriteString(temps);
                node_output_counts[current_pr]++;
            }
            else{
                unlinked_counter[current_pr]++;
            }
        }
    }
    if (codes.find(current_dx) != codes.end())
    {
        if (p->linked())
        {
            temps = v.SerializeAsString();
            node_output_files[current_dx]->WriteVarint32(temps.size());
            node_output_files[current_dx]->WriteString(temps);
            node_output_counts[current_dx]++;

        }
        else{
            unlinked_counter[current_dx]++;
        }

    }
    if (codes.find(v.drg()) != codes.end())
    {
        if (p->linked())
        {
            temps = v.SerializeAsString();
            node_output_files[v.drg()]->WriteVarint32(temps.size());
            node_output_files[v.drg()]->WriteString(temps);
            node_output_counts[v.drg()]++;

        }
        else{
            unlinked_counter[v.drg()]++;
        }
    }
}

void process_n3_edges(comphealth::Patient* p,std::unordered_map<std::string,std::string>& codes,unordered_map<string,CodedOutputStream*>& edge_output_files,unordered_map<string,int>& edge_output_counts){
    vector<comphealth::VisitEdge*> edge_vector;
    string temps,current_pr,current_dx;
    edge_vector.clear();
    get_revisit_edges(p,edge_vector);
    for (auto e:edge_vector){
        for(auto prcode:e->initial().prs()){
            current_pr = prcode.pcode();
            if (codes.find(current_pr) != codes.end())
            {
                temps = e->SerializeAsString();
                edge_output_files[current_pr]->WriteVarint32(temps.size());
                edge_output_files[current_pr]->WriteString(temps);
                edge_output_counts[current_pr]++;
            }
        }
        current_dx = e->initial().primary_diagnosis();
        if (codes.find(current_dx) != codes.end()){
            temps = e->SerializeAsString();
            edge_output_files[current_dx]->WriteVarint32(temps.size());
            edge_output_files[current_dx]->WriteString(temps);
            edge_output_counts[current_dx]++;

        }
        if (codes.find(e->initial().drg()) != codes.end()){
            temps = e->SerializeAsString();
            edge_output_files[e->initial().drg()]->WriteVarint32(temps.size());
            edge_output_files[e->initial().drg()]->WriteString(temps);
            edge_output_counts[e->initial().drg()]++;

        }
        delete e;
    }
}



void indexer::Indexer::index_DB()
{
    leveldb::DB* db;
    int count = 0;
    leveldb::Options options;
    comphealth::Patient *p;
    const std::string dbpath = config["DATASETS"][dataset]["ROOT"].string_value()+"/DB";
    leveldb::Status status = leveldb::DB::Open(options,dbpath, &db);
    if (!status.ok())
    {
        std::cerr << "Unable to open/create test database "<< dbpath << std::endl;
        std::cerr << status.ToString() << std::endl;
    }
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    p = new comphealth::Patient();
    for (it->SeekToFirst(); it->Valid(); it->Next()){
        count++;
        if (count % 1000000 == 0)
        {
            LOG(INFO) << "Finished " << count << std::endl;
            google::FlushLogFiles(google::INFO);
        }
        p->ParseFromString(it->value().ToString());
        if (N4){
            process_n4_patients(p,codes_hash,n4_output_files,n4_output_counts);
        }
        for (comphealth::Visit v:p->visits()){
            if (N1){
                process_n1_visit(v,codes_hash,n1_output_files,n1_output_counts);
            }
            if (N2){
                process_n2_nodes(p,v,codes_hash,n2_node_output_files,n2_node_output_counts,n2_unlinked_counter);
            }
            if (N3){
                process_n3_nodes(p,v,codes_hash,n3_node_output_files,n3_node_output_counts,n3_unlinked_counter);
            }
        }
        if (N2){
            process_n2_edges(p,codes_hash,n2_edge_output_files,n2_edge_output_counts);
        }
        if (N3){
            process_n3_edges(p, codes_hash, n3_edge_output_files, n3_edge_output_counts);
        }
    }
    LOG(INFO) << "Finished " << count << std::endl;
    google::FlushLogFiles(google::INFO);
}


void indexer::Indexer::close_indexer(){
    std::string k,hash_code;
    ofstream metadata;
    for(auto kh:codes_hash)
    {
        k = kh.first;
        hash_code = kh.second;
        metadata.open(index_folder_path+"/"+hash_code+".metadata",std::fstream::out | std::fstream::app);
        if (N4){
            metadata<<"N4\t"<<k<<"\t"<<n4_output_paths[k]<<"\t"<<n4_output_counts[k]<<endl;
            LOG(INFO)<<"N4\t"<<k<<"\t"<<n4_output_paths[k]<<"\t"<<n4_output_counts[k]<<endl;
            delete n4_output_files[k]; // Seems following order really matters.
            n4_output_stream[k]->Flush();
            n4_output_stream[k]->Close();
            delete n4_output_stream[k];
            close(n4_output_fds[k]);
        }
        if (N1){
            metadata<<"N1\t"<<k<<"\t"<<n1_output_paths[k]<<"\t"<<n1_output_counts[k]<<endl;
            LOG(INFO)<<"N1\t"<<k<<"\t"<<n1_output_paths[k]<<"\t"<<n1_output_counts[k]<<endl;
            delete n1_output_files[k]; // Seems following order really matters.
            n1_output_stream[k]->Flush();
            n1_output_stream[k]->Close();
            delete n1_output_stream[k];
            close(n1_output_fds[k]);
        }
        if (N2){
            metadata<<"N2\t"<<k<<"\t"<<n2_node_output_paths[k]<<"\t"<<n2_edge_output_paths[k]<<"\t"<<n2_unlinked_counter[k]<<"\t"<<n2_node_output_counts[k]<<"\t"<<n2_edge_output_counts[k]<<endl;
            LOG(INFO)<<"N2\t"<<k<<"\t"<<n2_node_output_paths[k]<<"\t"<<n2_edge_output_paths[k]<<"\t"<<n2_unlinked_counter[k]<<"\t"<<n2_node_output_counts[k]<<"\t"<<n2_edge_output_counts[k]<<endl;
            delete n2_node_output_files[k]; // Seems following order really matters.
            n2_node_output_stream[k]->Flush();
            n2_node_output_stream[k]->Close();
            delete n2_node_output_stream[k];
            close(n2_node_output_fds[k]);
            delete n2_edge_output_files[k]; // Seems following order really matters.
            n2_edge_output_stream[k]->Flush();
            n2_edge_output_stream[k]->Close();
            delete n2_edge_output_stream[k];
            close(n2_edge_output_fds[k]);
        }
        if (N3){
            metadata<<"N3\t"<<k<<"\t"<<n3_node_output_paths[k]<<"\t"<<n3_edge_output_paths[k]<<"\t"<<n3_unlinked_counter[k]<<"\t"<<n3_node_output_counts[k]<<"\t"<<n3_edge_output_counts[k]<<endl;
            LOG(INFO)<<"N3\t"<<k<<"\t"<<n3_node_output_paths[k]<<"\t"<<n3_edge_output_paths[k]<<"\t"<<n3_unlinked_counter[k]<<"\t"<<n3_node_output_counts[k]<<"\t"<<n3_edge_output_counts[k]<<endl;
            delete n3_node_output_files[k]; // Seems following order really matters.
            n3_node_output_stream[k]->Flush();
            n3_node_output_stream[k]->Close();
            delete n3_node_output_stream[k];
            close(n3_node_output_fds[k]);
            delete n3_edge_output_files[k]; // Seems following order really matters.
            n3_edge_output_stream[k]->Flush();
            n3_edge_output_stream[k]->Close();
            delete n3_edge_output_stream[k];
            close(n3_edge_output_fds[k]);
        }
       metadata.close();
    }
}