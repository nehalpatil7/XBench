#include "../TimescaleDB/TimescaleDB_Adapter.h"
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <any>
#include <chrono>
#include <cstdlib>

int main(int argc, char* argv[]) {
    int NUM_ROWS = 1000;
    unsigned short NUM_THREAD = 1;

    if (argc >= 2) {
        NUM_ROWS = std::stoi(argv[1]);
    }
    if (argc >= 3) {
        NUM_THREAD = static_cast<unsigned short>(std::stoi(argv[2]));
    }

    int N_ITERATION = NUM_ROWS;

    unsigned long int base_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();

    // Prepare dummy insert data for each thread
    std::vector<std::vector<std::vector<std::any>>> insertData(NUM_THREAD);

    for (unsigned short t = 0; t < NUM_THREAD; ++t) {
        std::vector<std::vector<std::any>> threadData;
        for (int i = 0; i < NUM_ROWS; ++i) {
            std::vector<std::any> row;
            // avoid duplicates - offset each timestamp by unique value
            unsigned long int ts = base_ts + (t * NUM_ROWS + i);
            row.push_back(ts);
            row.push_back(42 + i);
            threadData.push_back(row);
        }
        insertData[t] = threadData;
    }

    std::string SERVER_ADDR = "";
    std::string SERVER_PORT = "9493";
    std::string pattern = "unaryInsert_test";
    bool isDebug = true;

    std::vector<double> results = TimescaleDB_Adapter::unaryInsert(
                                    SERVER_ADDR,
                                    SERVER_PORT,
                                    NUM_THREAD,
                                    N_ITERATION,
                                    &insertData,
                                    pattern,
                                    isDebug);

    for (size_t i = 0; i < results.size(); i++) {
        std::cout << "Iteration " << i << " - Time taken (ms): " << results[i] << std::endl;
    }

    return 0;
}
