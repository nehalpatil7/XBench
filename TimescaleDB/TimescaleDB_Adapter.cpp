#include "TimescaleDB_Adapter.h"
#include "../include/BS_thread_pool.hpp"

// Threading library
static BS::thread_pool threadPool;

std::vector<double> TimescaleDB_Adapter::unaryQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(TimescaleDB_Adapter::unaryQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fmt::format("QUERY-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> TimescaleDB_Adapter::rangeQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(TimescaleDB_Adapter::rangeQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fmt::format("QUERY-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> TimescaleDB_Adapter::batchQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(TimescaleDB_Adapter::batchQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fmt::format("QUERY-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> TimescaleDB_Adapter::unaryInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create bench_db
    std::string dbName = "bench_db";
    TimescaleDB_Adapter::createDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(TimescaleDB_Adapter::unaryInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fmt::format("INSERT-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> TimescaleDB_Adapter::batchInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create bench_db
    std::string dbName = "bench_db";
    TimescaleDB_Adapter::createDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(TimescaleDB_Adapter::batchInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fmt::format("INSERT-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> TimescaleDB_Adapter::unaryQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    // Init TimescaleDB connection
    try {
        pqxx::connection conn(getConnectionString(SERVER_ADDR, SERVER_PORT, "bench_db"));

        // Clock init
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
        std::chrono::duration<double> timeCost;

        // Query target time
        unsigned long int queryTargetTime;

        for (int i = 0; i < N_ITERATION; i++) {
            queryTargetTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));

            if (timedOut) {
                if (isDebug) {
                    spdlog::warn("[UNARY QUERY] - There was an exception or operation has timed-out");
                }
                toReturn.push_back(0);
                continue;
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            try {
                pqxx::work txn(conn);
                pqxx::result result = txn.exec(
                    fmt::format("SELECT * FROM bench_db WHERE timestamp = to_timestamp({});", queryTargetTime)
                );
                txn.commit();

                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli>(timeCost).count());

                if (isDebug) {
                    spdlog::debug("[UNARY QUERY] - Querying: {} | Query Result: {}", queryTargetTime, result[0]["timestamp"].c_str());
                }
            }
            catch (const std::exception &e) {
                if (isDebug) {
                    spdlog::warn("[UNARY QUERY] - There was an exception or operation has timed-out: {}", e.what());
                }
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }

    }
    catch (const std::exception &e) {
        spdlog::error("[UNARY QUERY] - Connection error: {}", e.what());
        for (int i = 0; i < N_ITERATION; i++) {
            toReturn.push_back(0);
        }
    }
    return toReturn;
}

std::vector<double> TimescaleDB_Adapter::rangeQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    // Init TimescaleDB connection
    try {
        pqxx::connection conn(getConnectionString(SERVER_ADDR, SERVER_PORT, "bench_db"));

        // Clock init
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
        std::chrono::duration<double> timeCost;

        // Query target time
        unsigned long int queryStartTime, queryEndTime;

        for (int i = 0; i < N_ITERATION; i++) {
            queryStartTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));
            queryEndTime = std::any_cast<unsigned long int>(queryData->at(i).at(1));

            if (timedOut) {
                if (isDebug) {
                    spdlog::warn("[RANGE QUERY] - There was an exception or operation has timed-out");
                }
                toReturn.push_back(0);
                continue;
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            try {
                pqxx::work txn(conn);
                pqxx::result result = txn.exec(
                    fmt::format("SELECT * FROM bench_db WHERE timestamp >= to_timestamp({}) AND timestamp <= to_timestamp({});",
                                queryStartTime, queryEndTime)
                );
                txn.commit();

                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli>(timeCost).count());

                if (isDebug) {
                    std::vector<std::string> resultVector;
                    for (auto row : result) {
                        resultVector.push_back(row["timestamp"].c_str());
                    }
                    spdlog::debug("[RANGE QUERY] - From: {} to {} | Query Result: {}", queryStartTime, queryEndTime, fmt::join(resultVector, "\n"));
                }
            }
            catch (const std::exception &e) {
                if (isDebug) {
                    spdlog::warn("[RANGE QUERY] - There was an exception or operation has timed-out: {}", e.what());
                }
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    catch (const std::exception &e) {
        spdlog::error("[RANGE QUERY] - Connection error: {}", e.what());
        for (int i = 0; i < N_ITERATION; i++) {
            toReturn.push_back(0);
        }
    }
    return toReturn;
}

std::vector<double> TimescaleDB_Adapter::batchQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    // Init TimescaleDB connection
    try {
        pqxx::connection conn(getConnectionString(SERVER_ADDR, SERVER_PORT, "bench_db"));

        // Clock init
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
        std::chrono::duration<double> timeCost;

        for (int i = 0; i < N_ITERATION; i++) {
            // Extract target times into a list for IN clause
            std::string timeList;
            for (int j = 0; j < queryData->at(i).size(); j++) {
                if (j > 0) timeList += ", ";
                timeList += fmt::format("to_timestamp({})", std::any_cast<unsigned long int>(queryData->at(i).at(j)));
            }

            if (timedOut) {
                if (isDebug) {
                    spdlog::warn("[BATCH QUERY] - There was an exception or operation has timed-out");
                }
                toReturn.push_back(0);
                continue;
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            try {
                pqxx::work txn(conn);
                pqxx::result result = txn.exec(
                    fmt::format("SELECT * FROM bench_db WHERE timestamp IN ({});", timeList)
                );
                txn.commit();
                end = std::chrono::high_resolution_clock::now();

                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli>(timeCost).count());

                if (isDebug) {
                    std::vector<std::string> resultVector;
                    for (auto row : result) {
                        resultVector.push_back(row["timestamp"].c_str());
                    }
                    spdlog::debug("[BATCH QUERY] - Query Result: {}", fmt::join(resultVector, "\n"));
                }
            }
            catch (const std::exception &e) {
                if (isDebug) {
                    spdlog::warn("[BATCH QUERY] - There was an exception or operation has timed-out: {}", e.what());
                }
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    catch (const std::exception &e) {
        spdlog::error("[BATCH QUERY] - Connection error: {}", e.what());
        for (int i = 0; i < N_ITERATION; i++) {
            toReturn.push_back(0);
        }
    }
    return toReturn;
}

std::vector<double> TimescaleDB_Adapter::unaryInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    // Init TimescaleDB connection
    try {
        pqxx::connection conn(getConnectionString(SERVER_ADDR, SERVER_PORT, "bench_db"));

        // Clock init
        std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
        std::chrono::duration<double> timeCost;

        // Prepare the insert statement once
        std::string insertStmt = prepareInsertStatement(insertData->at(0).size());

        for (int i = 0; i < N_ITERATION; i++) {
            if (timedOut) {
                if (isDebug) {
                    spdlog::warn("[UNARY INSERT] - There was an exception or operation has timed-out");
                }
                toReturn.push_back(0);
                continue;
            }

            // Dispatch insert
            try {
                pqxx::work txn(conn);

                // Prepare parameters
                std::vector<std::string> params;

                // Timestamp
                unsigned long int timestamp = std::any_cast<unsigned long int>(insertData->at(i).at(0));
                params.push_back(fmt::format("to_timestamp({})", timestamp));

                // Other fields
                std::any currentMetric;
                for (int j = 1; j < insertData->at(i).size(); j++) {
                    currentMetric = insertData->at(i).at(j);
                    if (currentMetric.type() == typeid(std::string)) {
                        params.push_back(fmt::format("'{}'", std::any_cast<std::string>(currentMetric)));
                    }
                    else if (currentMetric.type() == typeid(int)) {
                        params.push_back(fmt::format("{}", std::any_cast<int>(currentMetric)));
                    }
                    else if (currentMetric.type() == typeid(double)) {
                        params.push_back(fmt::format("{}", std::any_cast<double>(currentMetric)));
                    }
                }

                // Execute the insert
                start = std::chrono::high_resolution_clock::now();
                txn.exec(fmt::format(fmt::runtime(insertStmt), fmt::join(params, ", ")));
                txn.commit();
                end = std::chrono::high_resolution_clock::now();

                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli>(timeCost).count());

                if (isDebug) {
                    spdlog::debug("[UNARY INSERT] - Insert Time: {} | Insert Result: OK", timestamp);
                }
            }
            catch (const std::exception &e) {
                if (isDebug) {
                    spdlog::warn("[UNARY INSERT] - There was an exception or operation has timed-out: {}", e.what());
                }
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    catch (const std::exception &e) {
        spdlog::error("[UNARY INSERT] - Connection error: {}", e.what());
        for (int i = 0; i < N_ITERATION; i++) {
            toReturn.push_back(0);
        }
    }
    return toReturn;
}

std::vector<double> TimescaleDB_Adapter::batchInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    try {
        pqxx::connection conn(getConnectionString(SERVER_ADDR, SERVER_PORT, "bench_db"));

        for (int i = 0; i < N_ITERATION; i++) {
            if (timedOut) {
                toReturn.push_back(0);
                continue;
            }

            try {
                pqxx::work txn(conn);

                std::string sql = "INSERT INTO bench_db VALUES ";
                std::vector<std::string> rowTuples;

                for (size_t j = 0; j < insertData->size(); j++) {
                    std::vector<std::string> colValues;

                    unsigned long int timestamp = std::any_cast<unsigned long int>(insertData->at(j).at(0));
                    colValues.push_back(fmt::format("to_timestamp({})", timestamp));

                    std::any currentMetric;
                    for (size_t k = 1; k < insertData->at(j).size(); k++) {
                        currentMetric = insertData->at(j).at(k);
                        if (currentMetric.type() == typeid(std::string)) {
                            colValues.push_back(fmt::format("'{}'", std::any_cast<std::string>(currentMetric)));
                        }
                        else if (currentMetric.type() == typeid(int)) {
                            colValues.push_back(fmt::format("{}", std::any_cast<int>(currentMetric)));
                        }
                        else if (currentMetric.type() == typeid(double)) {
                            colValues.push_back(fmt::format("{}", std::any_cast<double>(currentMetric)));
                        }
                    }

                    rowTuples.push_back(fmt::format("({})", fmt::join(colValues, ", ")));
                }
                sql += fmt::format("{} ON CONFLICT (timestamp) DO NOTHING;", fmt::join(rowTuples, ", "));

                auto start = std::chrono::high_resolution_clock::now();
                txn.exec(sql);
                txn.commit();
                auto end = std::chrono::high_resolution_clock::now();

                std::chrono::duration<double, std::milli> timeCost = end - start;
                toReturn.push_back(timeCost.count());

                if (isDebug) {
                    spdlog::debug("[BATCH INSERT] - Bulk insert completed successfully. SQL: {}", sql);
                }
            }
            catch (const std::exception &e) {
                spdlog::warn("[BATCH INSERT] - Exception encountered: {}", e.what());
                toReturn.push_back(0);
                timedOut = true; // Flag subsequent iterations as timed out.
            }
        }
    }
    catch (const std::exception &e) {
        spdlog::error("[BATCH INSERT] - Connection error: {}", e.what());
        for (int i = 0; i < N_ITERATION; i++) {
            toReturn.push_back(0);
        }
    }

    return toReturn;
}

void TimescaleDB_Adapter::createDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string dbName, std::vector<std::any> *singleRowData) {
    try {
        // First check if database exists
        {
            pqxx::connection conn(getConnectionString(SERVER_ADDR, SERVER_PORT));
            pqxx::work txn(conn);

            pqxx::result result = txn.exec(
                fmt::format("SELECT 1 FROM pg_database WHERE datname = '{}';", dbName)
            );

            if (result.empty()) {
                // Create database if it doesn't exist
                txn.commit();
                // Create new connection scope for database creation
                {
                    pqxx::connection adminConn(getConnectionString(SERVER_ADDR, SERVER_PORT));
                    pqxx::nontransaction ntxn(adminConn);
                    ntxn.exec(fmt::format("CREATE DATABASE {};", dbName));
                    ntxn.commit();
                } // adminConn goes out of scope here
            } else {
                txn.commit();
            }
        } // conn goes out of scope here

        // Now connect to the bench_db database to create the table
        {
            pqxx::connection dbConn(getConnectionString(SERVER_ADDR, SERVER_PORT, dbName));
            pqxx::work dbTxn(dbConn);

            // Create table if it doesn't exist
            std::string createTableSQL = R"(
                CREATE TABLE IF NOT EXISTS bench_db (
                    timestamp TIMESTAMPTZ NOT NULL,
            )";

            // Add columns based on the data types in singleRowData
            for (int i = 1; i < singleRowData->size(); i++) {
                std::any currentMetric = singleRowData->at(i);
                std::string colType;

                if (currentMetric.type() == typeid(std::string)) {
                    colType = "TEXT";
                }
                else if (currentMetric.type() == typeid(int)) {
                    colType = "INTEGER";
                }
                else if (currentMetric.type() == typeid(double)) {
                    colType = "DOUBLE PRECISION";
                }
                else {
                    colType = "TEXT"; // Default to TEXT for unknown types
                }

                createTableSQL += fmt::format("    col{} {} NOT NULL,\n", i - 1, colType);
            }

            // Add primary key and close the statement
            createTableSQL += "    PRIMARY KEY (timestamp)\n);";

            // Execute the create table statement
            dbTxn.exec(createTableSQL);

            // Convert to TimescaleDB hypertable
            dbTxn.exec("CREATE EXTENSION IF NOT EXISTS timescaledb;");

            // Check if it's already a hypertable
            pqxx::result result = dbTxn.exec(
                "SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'bench_db';"
            );

            if (result.empty()) {
                // Convert to hypertable if not already
                dbTxn.exec("SELECT create_hypertable('bench_db', 'timestamp', if_not_exists => TRUE);");
            }

            dbTxn.commit();
        } // dbConn goes out of scope here
    }
    catch (const std::exception &e) {
        spdlog::error("[CREATE DB] - Failed to create {}: {}", dbName, e.what());
    }
}

std::string TimescaleDB_Adapter::getConnectionString(std::string SERVER_ADDR, std::string SERVER_PORT, std::string dbName) {
    if (dbName.empty()) {
        return fmt::format("host={} port={} user=postgres password=postgres", SERVER_ADDR, SERVER_PORT);
    } else {
        return fmt::format("host={} port={} dbname={} user=postgres password=postgres", SERVER_ADDR, SERVER_PORT, dbName);
    }
}

std::string TimescaleDB_Adapter::prepareInsertStatement(int numFields) {
    std::string stmt = "INSERT INTO bench_db (timestamp";

    // Add column names
    for (int i = 1; i < numFields; i++) {
        stmt += fmt::format(", col{}", i - 1);
    }

    // Add values placeholder
    stmt += ") VALUES ({})";

    return stmt;
}

std::string TimescaleDB_Adapter::getTimeResolution(unsigned long int timestamp) {
    int count = 0;

    while (timestamp != 0) {
        timestamp /= 10;
        ++count;
    }

    if (count < 13) {
        return "1 second";
    }
    else if (count >= 13 && count < 16) {
        return "1 millisecond";
    }
    else if (count >= 16) {
        return "1 microsecond";
    }

    // Default
    return "1 second";
}
