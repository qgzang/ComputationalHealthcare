//
// Created by Akshay on 5/15/16.
//

#ifndef ANALYSIS_CINDEXER_H
#define ANALYSIS_CINDEXER_H
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
#include "leveldb/write_batch.h"



namespace cindexer
{
    void index_codes(std::string _filename,std::string _i,json11::Json _config,std::string _dataset);
}


#endif //ANALYSIS_CINDEXER_H
