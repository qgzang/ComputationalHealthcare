//
// Created by Akshay on 8/20/15.
//

#ifndef CPP_INDEXER_H
#define CPP_INDEXER_H
#include <iostream>
#include <fstream>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <string>
#include <ctime>
#include "../protos/pvisit.pb.h"
#include "../protos/penums.pb.h"
#include <sys/fcntl.h>
#include <unistd.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include "glog/logging.h"
#include "../pystring/pystring.h"
#include "../json11/json11.hpp"
#include "leveldb/db.h"

using namespace std;

namespace indexer{

    class Indexer{
    public:
        std::unordered_map<std::string,std::string> codes_hash;
        std::unordered_map<std::string,google::protobuf::io::FileOutputStream*> n4_output_stream,n1_output_stream,n2_node_output_stream,n2_edge_output_stream,n3_node_output_stream,n3_edge_output_stream;
        std::unordered_map<std::string,google::protobuf::io::CodedOutputStream*> n4_output_files,n1_output_files,n2_node_output_files,n2_edge_output_files,n3_node_output_files,n3_edge_output_files;
        std::unordered_map<std::string,int> n4_output_fds,n1_output_fds,n2_node_output_fds,n2_edge_output_fds,n3_node_output_fds,n3_edge_output_fds;
        std::unordered_map<std::string,std::string> n4_output_paths,n1_output_paths,n2_node_output_paths,n2_edge_output_paths,n3_node_output_paths,n3_edge_output_paths;
        std::unordered_map<std::string,int> n4_output_counts,n1_output_counts,n2_node_output_counts,n2_edge_output_counts,n2_unlinked_counter,n3_node_output_counts,n3_edge_output_counts,n3_unlinked_counter;
        string index_folder_path,dataset;
        bool N1,N2,N3,N4;
        json11::Json config;
        Indexer(std::unordered_map<std::string,std::string> _codes_hash,json11::Json _config,string _dataset)
        {
            config = _config;
            dataset = _dataset;
            codes_hash = _codes_hash;
            std::string opath,k,hash_code,output_path;
            index_folder_path = config["DATASETS"][dataset]["ROOT"].string_value()+"/INDEX/";
            N1 = false; // config["DATASETS"][dataset]["ANALYTICS"]["aggregate_visits"].bool_value();
            N2 = false; //config["DATASETS"][dataset]["ANALYTICS"]["aggregate_readmits"].bool_value();
            N3 = false; //config["DATASETS"][dataset]["ANALYTICS"]["aggregate_revisits"].bool_value();
            N4 = true; //config["DATASETS"][dataset]["ANALYTICS"]["aggregate_patients"].bool_value();
            LOG(INFO) <<"codes: " << codes_hash.size();
            google::FlushLogFiles(google::INFO);
            output_path = index_folder_path+"/";
            LOG(INFO) <<"output path: " << output_path;
            for(auto kp:codes_hash) {
                k = kp.first;
                hash_code = kp.second;
                if (N1){
                    opath = output_path + "/" + k + ".n1";
                    n1_output_paths[k] = opath;
                    n1_output_fds[k] = open(opath.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666); // caught an erro
                    n1_output_stream[k] = new google::protobuf::io::FileOutputStream(n1_output_fds[k]);
                    n1_output_files[k] = new google::protobuf::io::CodedOutputStream(n1_output_stream[k]);
                    n1_output_files[k]->WriteVarint32(1);
                }
                if (N4){
                    opath = output_path + "/" + k + ".n4";
                    n4_output_paths[k] = opath;
                    n4_output_fds[k] = open(opath.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666); // caught an erro
                    n4_output_stream[k] = new google::protobuf::io::FileOutputStream(n4_output_fds[k]);
                    n4_output_files[k] = new google::protobuf::io::CodedOutputStream(n4_output_stream[k]);
                    n4_output_files[k]->WriteVarint32(1);
                }
                if (N2){
                    opath = output_path + "/" + k + ".n2_node";
                    n2_node_output_paths[k] = opath;
                    n2_node_output_fds[k] = open(opath.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
                    n2_node_output_stream[k] = new google::protobuf::io::FileOutputStream(n2_node_output_fds[k]);
                    n2_node_output_files[k] = new google::protobuf::io::CodedOutputStream(n2_node_output_stream[k]);
                    n2_node_output_files[k]->WriteVarint32(1);
                    opath = output_path + "/" + k + ".n2_edge";
                    n2_edge_output_paths[k] = opath;
                    n2_edge_output_fds[k] = open(opath.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
                    n2_edge_output_stream[k] = new google::protobuf::io::FileOutputStream(n2_edge_output_fds[k]);
                    n2_edge_output_files[k] = new google::protobuf::io::CodedOutputStream(n2_edge_output_stream[k]);
                    n2_edge_output_files[k]->WriteVarint32(1);
                }
                if (N3){
                    opath = output_path + "/" + k + ".n3_node";
                    n3_node_output_paths[k] = opath;
                    n3_node_output_fds[k] = open(opath.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
                    n3_node_output_stream[k] = new google::protobuf::io::FileOutputStream(n3_node_output_fds[k]);
                    n3_node_output_files[k] = new google::protobuf::io::CodedOutputStream(n3_node_output_stream[k]);
                    n3_node_output_files[k]->WriteVarint32(1);
                    opath = output_path + "/" + k + ".n3_edge";
                    n3_edge_output_paths[k] = opath;
                    n3_edge_output_fds[k] = open(opath.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
                    n3_edge_output_stream[k] = new google::protobuf::io::FileOutputStream(n3_edge_output_fds[k]);
                    n3_edge_output_files[k] = new google::protobuf::io::CodedOutputStream(n3_edge_output_stream[k]);
                    n3_edge_output_files[k]->WriteVarint32(1);
                }
            }
        }
        void index_DB();
        void close_indexer();

    };

}

#endif //CPP_INDEXER_H
