//
// Created by Akshay on 2/15/16.
//

#ifndef ANALYSIS_VERIFY_H
#define ANALYSIS_VERIFY_H
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
#include "leveldb/db.h"
#include "../audit/auditor.h"
#include "../json11/json11.hpp"

namespace verify {
    void code_verify(json11::Json config,std::string dataset);
    void quick_verify(json11::Json config,std::string dataset);
}

#endif //ANALYSIS_VERIFY_H
