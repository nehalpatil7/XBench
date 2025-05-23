#!/usr/bin/env python3

import io
import random
import sys
import argparse
import hashlib
import pandas as pd                     # pip3 install pandas
import numpy as np
import os
from fastparquet import ParquetFile     # pip3 install fastparquet
import pyarrow.parquet as pq            # pip3 install pyarrow
import json
from datetime import datetime

# Client libraries
from XStore import XStoreClient         # Needs .py generated by Proto + pip3 install protobuf pyzmq
import XStore_pb2 as XStore_Proto
import pymongo                          # pip3 install pymongo
from pymongo import MongoClient
from influxdb import InfluxDBClient     # pip3 install influxdb
import psycopg2                         # pip3 install psycopg2-binary

# pd.set_option('display.max_columns', None)
# pd.set_option('expand_frame_repr', False)


# Ingest row group into TimescaleDB
def ingest_row_group_pg(args):
    idx, filename, db_config = args

    parquet_file = pq.ParquetFile(filename)
    df = parquet_file.read_row_group(idx).to_pandas()
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")

    total_rows = len(df)
    batch_size = max(1, total_rows // 2)

    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    cursor = conn.cursor()

    for start in range(0, total_rows, batch_size):
        chunk = df.iloc[start : start + batch_size]
        output = io.StringIO()
        chunk.to_csv(output, header=False, index=False)
        output.seek(0)
        cursor.copy_expert("COPY bench_db FROM STDIN WITH (FORMAT csv)", output)

    cursor.close()
    conn.close()

    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Row group {idx + 1} ingested.",
        flush=True,
    )


class ParquetOPS:
    def __init__(self):
        self.df = self.numRows = self.numGroups = self.rowsPerGroup = None

    def __atRowGroup(self, targetIdx):
        currMin = 0

        for i in range(self.numGroups):
            if currMin <= targetIdx < (self.rowsPerGroup * (i + 1)):
                return i
            currMin += self.rowsPerGroup

    def __segmentIdx(self, idxList):
        toReturn = {}
        for i in idxList:
            atRowGroup = self.__atRowGroup(i)
            if atRowGroup not in toReturn:
                toReturn[atRowGroup] = [i]
            else:
                toReturn[atRowGroup].append(i)
        return toReturn

    def __extractDataFromIdx(self, targetList):
        # Segment index into separate group to visit row group once
        segmentedIdxList = self.__segmentIdx(targetList)

        toWrite = pd.DataFrame()

        for rowGroupIdx in segmentedIdxList.keys():
            if rowGroupIdx is None:
                print(f"Warning: targetIdx out of range, skipping.")
                continue
            currIdxList = segmentedIdxList[rowGroupIdx]
            tempDF = self.df[rowGroupIdx].to_pandas()
            tempDF = tempDF.set_index(np.int64(tempDF.index + (self.rowsPerGroup * rowGroupIdx)))

            for item in currIdxList:
                toWrite = pd.concat([toWrite, tempDF.loc[[item]]])
        return toWrite.reset_index(drop=True)

    def genData(self, startTime, endTime, columnCount, outfileName, block=False, hashData=False):
        # Check if there's an existing data file
        if os.path.isfile(outfileName):
            print(f'[ERROR - GEN DATA] -- {outfileName} is already existed')
            exit(-1)

        if 'parquet' not in outfileName.lower():
            outfileName += '.parquet'

        columnLabels = [f'col{i}' for i in range(columnCount)]
        columnLabels.insert(0, 'timestamp')
        data = []

        if hashData:
            if block:
                # 10 minute gap between blocks - BTC block time
                for timeIdx in range(startTime, endTime + 1, 600):
                    temp = []
                    # First column will always be timestamp
                    temp.append(timeIdx)

                    # Create hash data for each column
                    for i in range(columnCount):
                        temp.append(hashlib.shake_256(f'{timeIdx}-{i}'.encode()).hexdigest(128))

                    # Append to master list
                    data.append(temp)

                    # Dumps to CSV every 10K rows
                    if len(data) == 10_000:
                        df = pd.DataFrame(data, columns=columnLabels)
                        if not os.path.isfile(outfileName):
                            df.to_parquet(outfileName, engine='fastparquet', compression='zstd')
                        else:
                            df.to_parquet(outfileName, engine='fastparquet', compression='zstd', append=True)

                        # Reset master list
                        data = []

            else:
                for timeIdx in range(startTime, endTime + 1):
                    temp = []
                    # First column will always be timestamp
                    temp.append(timeIdx)

                    # Create hash data for each column
                    for i in range(columnCount):
                        temp.append(hashlib.md5(f'{timeIdx}-{i}'.encode()).hexdigest())

                    # Append to master list
                    data.append(temp)

                    # Dumps to CSV every 1M rows
                    if len(data) == 1_000_000:
                        df = pd.DataFrame(data, columns=columnLabels)
                        if not os.path.isfile(outfileName):
                            df.to_parquet(outfileName, engine='fastparquet', compression='zstd')
                        else:
                            df.to_parquet(outfileName, engine='fastparquet', compression='zstd', append=True)

                        # Reset master list
                        data = []

        else:
            for timeIdx in range(startTime, endTime + 1):
                temp = []
                # First column will always be timestamp
                temp.append(timeIdx)

                # Create hash data for each column
                temp += np.random.uniform(low=-999_999, high=999_999, size=columnCount).tolist()

                # Append to master list
                data.append(temp)

                # Dumps to CSV every 1M rows
                if len(data) == 1_000_000:
                    df = pd.DataFrame(data, columns=columnLabels)
                    if not os.path.isfile(outfileName):
                        df.to_parquet(outfileName, engine='fastparquet', compression='zstd')
                    else:
                        df.to_parquet(outfileName, engine='fastparquet', compression='zstd', append=True)

                    # Reset master list
                    data = []

        # Dumps remaining data to CSV
        df = pd.DataFrame(data, columns=columnLabels)

        if not os.path.isfile(outfileName):
            df.to_parquet(outfileName, engine='fastparquet', compression='zstd')
        else:
            df.to_parquet(outfileName, engine='fastparquet', compression='zstd', append=True)

    def ingestAll(self, db_name, ip_address, port_number, batchSize, filename):
        port_number = int(port_number)
        masterDF = pq.ParquetFile(filename)

        if db_name == "XSTORE":
            xstoreClient = XStoreClient(ip_address, port_number)

            # Create DB
            createDBStatus = xstoreClient.createDB("BENCH_DB", XStore_Proto.timeUnit.Value("SECOND"), 1024+256)
            print(createDBStatus)

            for df in masterDF.iter_batches(batch_size=batchSize):
                dataList = []
                tempDF = df.to_pandas()

                for i in tempDF.itertuples():
                    timestamp = i[1]
                    data = [item for item in i[2:]]
                    currRow = {'timestamp': timestamp, 'data': data}
                    dataList.append(currRow)
                insertResult = xstoreClient.rangeInsert(dataList, "BENCH_DB")
                print(insertResult)

        elif db_name == "MONGODB":
            mongoClient = MongoClient(host=ip_address, port=port_number)
            mongoDB = mongoClient['BENCH_DB']

            # Create TS collection
            try:
                mongoClient.drop_database('BENCH_DB')
                mongoDB.create_collection('BENCH_DB', timeseries={'timeField': 'timestamp'})
            except pymongo.errors.CollectionInvalid:
                print("Time-series collection is already existed")
            mongoCollection = mongoDB['BENCH_DB']

            for df in masterDF.iter_batches(batch_size=batchSize):
                tempDF = df.to_pandas()
                tempDF['json'] = tempDF.apply(lambda x: x.to_json(), axis=1)
                dataList = [json.loads(i) for i in tempDF['json'].tolist()]
                for i in range(len(dataList)):
                    dataList[i]['timestamp'] = datetime.fromtimestamp(dataList[i]['timestamp'])
                insertResult = mongoCollection.insert_many(dataList)

                if insertResult.acknowledged == True:
                    print(f"Inserted {batchSize:,.2f} into DB")
                else:
                    print(f"Error on inserted into DB")
                    exit(1)

        elif db_name == "INFLUXDB":
            influxClient = InfluxDBClient(ip_address, int(port_number), database="BENCH_DB", gzip=True)

            # Create DB
            influxClient.drop_database("BENCH_DB")
            influxClient.create_database("BENCH_DB")

            for df in masterDF.iter_batches(batch_size=batchSize):
                dataList = []
                tempDF = df.to_pandas()

                for i in tempDF.itertuples():
                    currRow = {"measurement": "BENCH_DB", "time": datetime.fromtimestamp(i[1]), "fields": {}}
                    data = [item for item in i[2:]]

                    for idx, element in enumerate(data):
                        currRow["fields"][f"col{idx}"] = element
                    dataList.append(currRow)
                insertResult = influxClient.write_points(dataList)
                if insertResult == True:
                    print(f"Inserted {batchSize} into DB")
                else:
                    print(f"Error on inserted into DB")
                    exit(1)

        elif db_name == "TIMESCALEDB":
            log_file_path = "ingest.log"
            open(log_file_path, "a").close()

            db_config = {
                "host": ip_address,
                "port": port_number,
                "user": "postgres",
                "dbname": "bench_db",
            }

            parquet_file = pq.ParquetFile(filename)
            num_row_groups = parquet_file.num_row_groups

            for i in range(num_row_groups):
                ingest_row_group_pg((i, filename, db_config))

            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] All row groups successfully ingested into TimescaleDB.",
                flush=True,
            )

    def createBasicQuery(self, offset, sampleSize, nClients, startTime, endTime, queryType='UNARY_SEQ', batchIter=None):
        # Batch size default
        if batchIter is None:
            batchIter = 10

        if queryType == 'UNARY_SEQ':
            for i in range(nClients):
                currStart = (startTime + offset) + (i * sampleSize)

                # Sequential
                targetList = [i for i in range(currStart, currStart + sampleSize)]

                # Prepare DF to write
                toWrite = pd.DataFrame(targetList, columns=['timestamp'])
                toWrite['timestamp'] = toWrite['timestamp'].astype(np.uint64)

                # Write to file
                fName = f'BASIC-QUERY-UNARY-SEQ_Client-{i}_{sampleSize}_{currStart}.parquet'
                toWrite.to_parquet(fName, engine='fastparquet', compression='zstd')
                print(f'File written: {fName}')

        elif queryType == 'UNARY_RAND':
            # Data for all N clients
            randData = random.sample(range(startTime, endTime), nClients * sampleSize)

            # Segment data for each client
            for i in range(nClients):
                currStart = i * sampleSize
                currEnd = (i * sampleSize) + sampleSize
                targetList = randData[currStart:currEnd]

                # Prepare DF to write
                toWrite = pd.DataFrame(targetList, columns=['timestamp'])
                toWrite['timestamp'] = toWrite['timestamp'].astype(np.uint64)

                # Write to file
                fName = f'BASIC-QUERY-UNARY-RAND_Client-{i}_{sampleSize}_{startTime}-{endTime}.parquet'
                toWrite.to_parquet(fName, engine='fastparquet', compression='zstd')
                print(f'File written: {fName}')

        elif queryType == 'BATCH_SEQ':
            for i in range(nClients):
                sTime = [random.randrange(startTime, endTime - (sampleSize * batchIter)) + (j * sampleSize) for j in range(batchIter)]
                eTime = [j + sampleSize for j in sTime]

                # Prepare DF to write
                toWrite = pd.DataFrame(zip(sTime, eTime), columns=['startTime', 'endTime'])
                toWrite['startTime'] = toWrite['startTime'].astype(np.uint64)
                toWrite['endTime'] = toWrite['endTime'].astype(np.uint64)

                # Write to file
                fName = f'BASIC-QUERY-BATCH-SEQ_Client-{i}_{batchIter}-{sampleSize}_{startTime}-{endTime}.parquet'
                toWrite.to_parquet(fName, engine='fastparquet', compression='zstd')
                print(f'File written: {fName}')

        elif queryType == 'BATCH_RAND':
            # Data for all N clients
            randData = random.sample(range(startTime, endTime), nClients * sampleSize * batchIter)

            # Segment data for each client
            for i in range(nClients):
                thisStart = [(i * sampleSize) + (t * sampleSize) for t in range(batchIter)]
                thisEnd = [t + sampleSize for t in thisStart]

                targetList = [randData[j[0]:j[1]] for j in zip(thisStart, thisEnd)]

                # Prepare DF to write
                toWrite = pd.DataFrame(targetList)
                toWrite = toWrite.astype(np.uint64)
                toWrite.columns = toWrite.columns.astype(str)

                # Write to file
                fName = f'BASIC-QUERY-BATCH-RAND_Client-{i}_{batchIter}-{sampleSize}_{startTime}-{endTime}.parquet'
                toWrite.to_parquet(fName, engine='fastparquet', compression='zstd')
                print(f'File written: {fName}')

    def createBasicInsert(self, offset, sampleSize, nClients, inputFile, insertType='SEQ', batchIter=None):
        if batchIter is None:
            batchIter = 10

        # Read parquet file
        self.df = ParquetFile(inputFile)

        # Parquet metadata
        self.numRows = self.df.count()
        self.numGroups = len(self.df.row_groups)
        self.rowsPerGroup = self.df[0].count()

        if insertType == 'UNARY_SEQ':
            for i in range(nClients):
                startRange = offset + (i * sampleSize)
                targetList = [i for i in range(startRange, startRange + sampleSize)]

                # Extract data from main DF based on targetList
                toWrite = self.__extractDataFromIdx(targetList)
                toWrite['timestamp'] = toWrite['timestamp'].astype(np.uint64)

                # Write to file
                fName = f'BASIC-INSERT-UNARY-SEQ_Client-{i}_{sampleSize}_{startRange}.parquet'
                toWrite.to_parquet(fName, engine='fastparquet', compression='zstd')
                print(f'File written: {fName}')

        elif insertType == 'UNARY_RAND':
            # Random indexes for all N clients
            randData = random.sample(range(0, self.numRows), nClients * sampleSize)

            for i in range(nClients):
                currStart = i * sampleSize
                currEnd = (i * sampleSize) + sampleSize
                targetList = randData[currStart:currEnd]

                # Extract data from main DF based on targetList
                toWrite = self.__extractDataFromIdx(targetList)
                toWrite['timestamp'] = toWrite['timestamp'].astype(np.uint64)

                # Write to file
                fName = f'BASIC-INSERT-UNARY-RAND_Client-{i}_{sampleSize}_{0}-{self.numRows}.parquet'
                toWrite.to_parquet(fName, engine='fastparquet', compression='zstd')
                print(f'File written: {fName}')

        elif insertType == 'BATCH_SEQ':
            for i in range(nClients):
                startRange = offset + (i * sampleSize * batchIter)
                endRange = startRange + (sampleSize * batchIter)

                targetList = [i for i in range(startRange, endRange)]

                # Extract data from main DF based on targetList
                toWrite = self.__extractDataFromIdx(targetList)
                toWrite['timestamp'] = toWrite['timestamp'].astype(np.uint64)

                # Write to file
                fName = f'BASIC-INSERT-BATCH-SEQ_Client-{i}_{batchIter}-{sampleSize}_{startRange}-{endRange}.parquet'
                toWrite.to_parquet(fName, engine='fastparquet', compression='zstd')
                print(f'File written: {fName}')

        elif insertType == 'BATCH_RAND':
            # Random indexes timestamp for all N clients
            randData = random.sample(range(0, self.numRows), nClients * sampleSize * batchIter)

            for i in range(nClients):
                startRange = i * sampleSize * batchIter
                endRange = startRange + (sampleSize * batchIter)

                targetList = [j for j in randData[startRange:endRange]]

                # Extract data from main DF based on targetList
                toWrite = self.__extractDataFromIdx(targetList)
                toWrite['timestamp'] = toWrite['timestamp'].astype(np.uint64)

                # Write to file
                fName = f'BASIC-INSERT-BATCH-RAND_Client-{i}_{batchIter}-{sampleSize}_{startRange}-{endRange}.parquet'
                toWrite.to_parquet(fName, engine='fastparquet', compression='zstd')
                print(f'File written: {fName}')

    def createAggQuery(self, offset, sampleSize, nClients, startTime, endTime, startTargetColIdx, endTargetColIdx, queryType='AGG_MIN', batchIter=None):
        # Batch size default
        if batchIter is None:
            batchIter = 10

        if queryType in ['AGG_MIN', 'AGG_MAX', 'AGG_SUM', 'AGG_AVG']:
            for i in range(nClients):
                sTime = [random.randrange(startTime, endTime - (sampleSize * batchIter)) + (j * sampleSize) for j in range(batchIter)]
                eTime = [j + sampleSize for j in sTime]
                targetCols = [random.randint(startTargetColIdx, endTargetColIdx - 1) for _ in range(batchIter)]

                # Prepare DF to write
                toWrite = pd.DataFrame(zip(sTime, eTime, targetCols), columns=['startTime', 'endTime', 'targetCol'])
                toWrite['startTime'] = toWrite['startTime'].astype(np.uint64)
                toWrite['endTime'] = toWrite['endTime'].astype(np.uint64)
                toWrite['targetCol'] = toWrite['targetCol'].astype(np.uint64)

                # Write to file
                fName = f'AGG-{queryType.split("_")[1]}-QUERY_Client-{i}_{batchIter}-{sampleSize}_{startTime}-{endTime}.parquet'
                toWrite.to_parquet(fName, engine='fastparquet', compression='zstd')
                print(f'File written: {fName}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='XBench Data Utilities')

    # Data generator
    subParser = parser.add_subparsers(title='commands', dest='command')
    dataGenerator = subParser.add_parser('generate', add_help=False, description='Synthetic data generator for benchmarking suite')
    dataGenerator.add_argument('-t', '--data-type', type=str, required=True, help='Type of data to be generated (numeric or hash (str)', choices=['numeric', 'hash'])
    dataGenerator.add_argument('-s', '--start-time', type=int, required=True, help='Start time in Epoch time format (inclusive)')
    dataGenerator.add_argument('-e', '--end-time', type=int, required=True, help='End time in Epoch time format (inclusive)')
    dataGenerator.add_argument('-c', '--column-count', type=int, required=True, help='Number of columns')
    dataGenerator.add_argument('-o', '--outfile_name', type=str, required=True, help='Name of the generated Parquet data file (With .parquet extension)')
    dataGenerator.add_argument('-b', '--block', action='store_true', help='Blockchain block type data generation')

    # Data preparer
    #   READ workload
    readWorkload = subParser.add_parser('queryWorkload', add_help=True, description='Data preparer for READ workloads')
    readWorkload.add_argument('-t', '--type', type=str, required=True, help='Workload type', choices=['UNARY_SEQ', 'UNARY_RAND', 'BATCH_SEQ', 'BATCH_RAND'])
    readWorkload.add_argument('-o', '--offset', type=int, required=True, help='Offset/Skip N timestamps')
    readWorkload.add_argument('-s', '--sample-size', type=int, required=True, help='Number of timestamps')
    readWorkload.add_argument('-n', '--num-client', type=int, required=True, help='Number of client (Each client has its own input data file)')
    readWorkload.add_argument('-st', '--start-time', type=int, required=True, help='Start range of epoch time')
    readWorkload.add_argument('-et', '--end-time', type=int, required=True, help='End range of epoch time')
    readWorkload.add_argument('-b', '--batch-iter', type=int, required=False, help='Batch iteration (Only applicable to RANGE/BATCH workloads')

    #   WRITE workload
    writeWorkload = subParser.add_parser('insertWorkload', add_help=True, description='Data preparer for WRITE workloads')
    writeWorkload.add_argument('-t', '--type', type=str, required=True, help='Workload type', choices=['UNARY_SEQ', 'UNARY_RAND', 'BATCH_SEQ', 'BATCH_RAND'])
    writeWorkload.add_argument('-o', '--offset', type=int, required=True, help='Offset/Skip N number of rows')
    writeWorkload.add_argument('-s', '--sample-size', type=int, required=True, help='Number of rows')
    writeWorkload.add_argument('-n', '--num-client', type=int, required=True, help='Number of client (Each client has its own input data file)')
    writeWorkload.add_argument('-i', '--input-data', type=str, required=True, help='Input parquet file')
    writeWorkload.add_argument('-b', '--batch-iter', type=int, required=False, help='Batch iteration (Only applicable to RANGE/BATCH workloads)')

    #   AGGREGATE workload
    aggWorkload = subParser.add_parser('aggWorkload', add_help=True, description='Data preparer for READ workloads')
    aggWorkload.add_argument('-t', '--type', type=str, required=True, help='Workload type', choices=['AGG_MIN', 'AGG_MAX', 'AGG_SUM', 'AGG_AVG'])
    aggWorkload.add_argument('-o', '--offset', type=int, required=True, help='Offset/Skip N timestamps')
    aggWorkload.add_argument('-s', '--sample-size', type=int, required=True, help='Number of timestamps')
    aggWorkload.add_argument('-n', '--num-client', type=int, required=True, help='Number of client (Each client has its own input data file)')
    aggWorkload.add_argument('-st', '--start-time', type=int, required=True, help='Start range of epoch time')
    aggWorkload.add_argument('-et', '--end-time', type=int, required=True, help='End range of epoch time')
    aggWorkload.add_argument('-sc', '--start-column', type=int, required=True, help='Start range of column index')
    aggWorkload.add_argument('-ec', '--end-column', type=int, required=True, help='End range of column index')
    aggWorkload.add_argument('-b', '--batch-iter', type=int, required=False, help='Batch iteration')

    #   INGEST ALL
    ingestAll = subParser.add_parser('ingestAll', add_help=True, description='Data ingestor for benchmarking suite')
    ingestAll.add_argument('-d', '--db_name', type=str, required=True, help='DB name', choices=['XSTORE', 'MONGODB', 'INFLUXDB', 'TIMESCALEDB'])
    ingestAll.add_argument('-i', '--ip_address', type=str, required=True, help='IP address of DB server')
    ingestAll.add_argument('-p', '--port_number', type=int, required=True, help='Port number of DB server')
    ingestAll.add_argument('-b', '--batch_size', type=int, required=True, help='Number of record per batch')
    ingestAll.add_argument('-f', '--file_name', type=str, required=True, help='Parquet data file use to insert to DB')

    # If no argument given -> Print help msg
    if len(sys.argv) == 1:
        parser.print_help()
        exit(1)

    # Parsing args
    args = parser.parse_args()
    parquetOps = ParquetOPS()

    # HANDLE data generator
    if args.command == 'generate':
        if args.data_type == "hash":
            parquetOps.genData(args.start_time, args.end_time, args.column_count, args.outfile_name, args.block, True)
        else:
            parquetOps.genData(args.start_time, args.end_time, args.column_count, args.outfile_name, args.block, False)

    # Bulk data ingestor
    elif args.command == 'ingestAll':
        parquetOps.ingestAll(db_name=args.db_name, ip_address=args.ip_address, port_number=args.port_number, batchSize=args.batch_size, filename=args.file_name)

    # BASIC QUERIES
    elif args.command == 'queryWorkload':
        parquetOps.createBasicQuery(offset=args.offset, sampleSize=args.sample_size, nClients=args.num_client, startTime=args.start_time, endTime=args.end_time, queryType=args.type, batchIter=args.batch_iter)

    # BASIC INSERTS
    elif args.command == 'insertWorkload':
        parquetOps.createBasicInsert(offset=args.offset, sampleSize=args.sample_size, nClients=args.num_client, inputFile=args.input_data, insertType=args.type, batchIter=args.batch_iter)

    # AGGREGATE QUERIES
    elif args.command == 'aggWorkload':
        parquetOps.createAggQuery(offset=args.offset, sampleSize=args.sample_size, nClients=args.num_client, startTime=args.start_time, endTime=args.end_time, startTargetColIdx=args.start_column, endTargetColIdx=args.end_column, queryType=args.type, batchIter=args.batch_iter)
