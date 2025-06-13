# XBench - A distributed benchmarking framework for TSDB

**Illinois Institute of Technology**\
**Data-Intensive Distributed Systems Laboratory**

Author: Lan Nguyen (Lnguyen18@hawk.iit.edu)\
Co-author: Nehal Patil (npatil14@hawk.iit.edu)\
Advisor: Ioan Raicu (iraicu@iit.edu)

# Description
Source code for project XBench and its related work.

# System Requirements
* Operating systems: Linux Ubuntu 22.04 LTS
* Compiler: g++ 11.4.0 &#40;C++ 17 standard&#41;
* Google Protocol Buffer v3.18.1 or higher
* ZeroMQ CPP v4.8.1 or higher

# Testing
* If the code executes but returns zeros, try executing this cmd on the client machine
> ./benchmarkClient {insertBench} -i {SERVER_IP | 127.0.0.1} -p 9493 -d {DB_NAME_ALL_CAPS} -t 1 -e {UNARY_SEQ} -n 1000 -b 0 -ia {current_timestamp + 10} --debug

# How to Install
* [Installation Wiki]&#40;[installationWiKi](https://gitlab.com/lvn2007/XStore/-/wikis/installation)&#41;

# How to Use
* [XStore API]&#40;[XBenchAPIs](https://gitlab.com/lvn2007/XStore/-/wikis/API);
* [Data Structure]&#40;[XStoreDataStructures](https://gitlab.com/lvn2007/XStore/-/wikis/Data-Structure);


[//]: # (This work has been published to:)

[//]: # ()
[//]: # (<details open><summary>Accelerating CRUD with Chrono Dilation for Time-Series Storage Systems:</summary>)

[//]: # ()
[//]: # (```)

[//]: # (Lan Nguyen, Ioan Raicu.)

[//]: # (“Accelerating CRUD with Chrono Dilation for Time-Series Storage Systems”,)

[//]: # (IEEE/ACM Supercomputing/SC 2023)

[//]: # (```)

[//]: # ()
[//]: # (* Poster: [Link to Poster]&#40;https://sc23.supercomputing.org/proceedings/src_poster/poster_files/spostg124s3-file1.pdf&#41;)

[//]: # (* 2-page Summary: [Link to Extended Abstract]&#40;https://sc23.supercomputing.org/proceedings/src_poster/poster_files/spostg124s3-file2.pdf&#41;)

[//]: # (* Presentation: [Link to Presentation]&#40;https://youtu.be/kYd0wFB3Zec&#41;)

[//]: # ()
[//]: # (</details>)

[//]: # ()
[//]: # (# System Requirements)

[//]: # (* Operating systems: Linux Ubuntu 22.04 LTS)

[//]: # (* Compiler: g++ 11.4.0 &#40;C++ 17 standard&#41;)

[//]: # (* Google Protocol Buffer v3.18.1 or higher)

[//]: # (* ZeroMQ CPP v4.8.1 or higher)

[//]: # ()
[//]: # (# How to Install)

[//]: # (* [Installation Wiki]&#40;https://gitlab.com/lvn2007/XStore/-/wikis/installation&#41;)

[//]: # ()
[//]: # (# How to Use)

[//]: # (* [XStore API]&#40;https://gitlab.com/lvn2007/XStore/-/wikis/api&#41;)

[//]: # (* [Data Structure]&#40;https://gitlab.com/lvn2007/XStore/-/wikis/Data%20Structure&#41;)

# Notes

<details open><summary>To tar a backup:</summary>

```console
sudo tar -I "zstd -T0 -19" -cSvf MongoDB_BENCH_DB.tar.zst MongoDB_BENCH_DB
```

</details>

<details open><summary>To upload to Chameleon Object Store:</summary>

```console
swift --os-auth-type v3applicationcredential --auth-version 3 --os-auth-url https://chi.uc.chameleoncloud.org:5000/v3 --os-application-credential-id [cred-id] --os-application-credential-secret [cred-secret] upload XStore_ExperimentDataset_SMALL --segment-size 4831838208 --segment-container XStore_ExperimentDataset_SMALL 10Y_1-Col_Experiments.tar.zst
```

* `os-application-credential-id/secret` - Obtain from Chameleon portal under `Identity/Applications Credentials`

</details>

<details open><summary>To tar a backup:</summary>

```console
sudo tar -I "zstd -T0 -19" -cSvf MongoDB_BENCH_DB.tar.zst MongoDB_BENCH_DB
```

</details>

<details open><summary>Backup/Restore DB:</summary>

* MongoDB:
    ```console
    > mongodump --archive=BENCH_DB --db BENCH_DB --host 127.0.0.1 --port 9491
    > mongorestore --host 127.0.0.1 --port 9491 --nsInclude=BENCH_DB.BENCH_DB --archive --drop
    ```

* InfluxDB:
    ```console
    > influxd backup -portable -db BENCH_DB -host 127.0.0.1:9492 InfluxDB_BENCH_DB
    > influxd restore -portable -db BENCH_DB -host 127.0.0.1:9492 InfluxDB_BENCH_DB
    ```

* TimescaleDB:
    ```console
    > pg_basebackup -h localhost -p 9493 -U postgres -F t -Z 5 -P --wal-method=stream -D 'timescaleDB_backup' && cd timescaleDB_backup && tar -cf - backup_manifest base.tar.gz pg_wal.tar.gz | pv | zstd -19 -T0 -o TimescaleDB_BENCH_DB.tar.zst && cd ..
    > sudo systemctl stop postgresql@14-main && \
        sudo rm -rf /NVME_RAID-0/postgresql && \
        mkdir -p /NVME_RAID-0/postgresql/pg_wal && \
        cp backups/backup_manifest /NVME_RAID-0/postgresql/ && \
        tar -xzf backups/base.tar.gz -C /NVME_RAID-0/postgresql && \
        tar -xzf backups/pg_wal.tar.gz -C /NVME_RAID-0/postgresql/pg_wal && \
        sudo chown -R postgres:postgres /NVME_RAID-0/postgresql && \
        sudo chmod 700 /NVME_RAID-0/postgresql && \
        sudo systemctl start postgresql@14-main
    ```
</details>

<details open><summary>To generate synthetic data</summary>

```console
python3 Parquet_OPS.py generate -t numeric -s 946684800 -e 1262303999 -c 128 -o benchData.parquet
```

</details>

<details open><summary>To generate workload datasets:</summary>

Specifications:
- [1, 2, 4, 8, 16, 32] threads each node
- 8 client nodes

* UNARY QUERY - SEQ
```console
python3 Parquet_OPS.py queryWorkload -t UNARY_SEQ -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0
python3 Parquet_OPS.py queryWorkload -t UNARY_SEQ -o 32000 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0
python3 Parquet_OPS.py queryWorkload -t UNARY_SEQ -o 64000 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0
...
```

* UNARY QUERY - RAND (Same cmd for all client nodes)
```console
python3 Parquet_OPS.py queryWorkload -t UNARY_RAND -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 0
```

* BATCH QUERY - SEQ
```console
python3 Parquet_OPS.py queryWorkload -t BATCH_SEQ -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 10
python3 Parquet_OPS.py queryWorkload -t BATCH_SEQ -o 320000 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 10
python3 Parquet_OPS.py queryWorkload -t BATCH_SEQ -o 640000 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 10
...
```

* BATCH QUERY - RAND (Same cmd for all client nodes)
```console
python3 Parquet_OPS.py queryWorkload -t BATCH_RAND -o 0 -s 1000 -n 32 -st 946684800 -et 1262303999 -b 10
```

* UNARY INSERT - SEQ
```console
python3 Parquet_OPS.py insertWorkload -t UNARY_SEQ -o 0 -s 1000 -n 32 -i hashData.parquet -b 0
python3 Parquet_OPS.py insertWorkload -t UNARY_SEQ -o 32000 -s 1000 -n 32 -i hashData.parquet -b 0
python3 Parquet_OPS.py insertWorkload -t UNARY_SEQ -o 64000 -s 1000 -n 32 -i hashData.parquet -b 0
...
```

* UNARY INSERT - RAND (Same cmd for all client nodes)
```console
python3 Parquet_OPS.py insertWorkload -t UNARY_RAND -o 0 -s 1000 -n 32 -i hashData.parquet -b 0
```

* BATCH INSERT - SEQ
```console
python3 Parquet_OPS.py insertWorkload -t BATCH_SEQ -o 0 -s 1000 -n 32 -i hashData.parquet -b 10
python3 Parquet_OPS.py insertWorkload -t BATCH_SEQ -o 320000 -s 1000 -n 32 -i hashData.parquet -b 10
python3 Parquet_OPS.py insertWorkload -t BATCH_SEQ -o 640000 -s 1000 -n 32 -i hashData.parquet -b 10
...
```

* BATCH INSERT - RAND (Same cmd for all client nodes)
```console
python3 Parquet_OPS.py insertWorkload -t BATCH_RAND -o 0 -s 1000 -n 32 -i hashData.parquet -b 10
```

</details>
