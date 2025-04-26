#ifndef XSTORE_MONGODB_ADAPTER_H
#define XSTORE_MONGODB_ADAPTER_H

#include "../include/Base_Adapter.h"
#include "../include/utilities.h"
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/list.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"

class MongoDB_Adapter: public Base_Adapter {
public:
    // UNARY QUERY
    //  Supports both SEQ & RAND access patterns
    static std::vector<double> unaryQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug=false);

    // RANGE QUERY
    //  A batch of queries with SEQ access pattern
    static std::vector<double> rangeQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug=false);

    // BATCH QUERY
    //  A batch of queries with RAND access pattern
    static std::vector<double> batchQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug=false);

    // UNARY INSERT
    //  Supports both SEQ & RAND access patterns
    static std::vector<double> unaryInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug=false);

    // BATCH INSERT
    //  A batch of inserts with SEQ/RAND access pattern
    static std::vector<double> batchInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug=false);

    // RANGE MIN
    //  A batch of aggregate query (MIN/MAX/SUM/AVG)
    static std::vector<double> aggQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug=false);

private:
    mongocxx::instance instance{};

    // QUERY
    static std::vector<double> unaryQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug);
    static std::vector<double> rangeQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug);
    static std::vector<double> batchQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug);

    // INSERT
    static std::vector<double> unaryInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug);
    static std::vector<double> rangeInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug);
    static std::vector<double> batchInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug);

    // AGGREGATE
    static std::vector<double> aggQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, std::string aggType, bool isDebug);

    static void createDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string dbName, std::vector<std::any> *singleRowData);
    static std::string getTimeResolution(unsigned long int timestamp);
    static bsoncxx::builder::basic::document makeSingleDocument(std::vector<std::any> *insertData);
};

#endif //XSTORE_MONGODB_ADAPTER_H
