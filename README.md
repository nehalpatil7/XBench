# XBench - A distributed benchmarking framework for TSDB

# Description
Source code for project XBench and its related work.

**Illinois Institute of Technology**  
**Data-Intensive Distributed Systems Laboratory**

Author: Lan Nguyen (Lnguyen18@hawk.iit.edu)\
Advisor: Ioan Raicu (iraicu@iit.edu)

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
    mongodump --archive=BENCH_DB --db BENCH_DB --host 127.0.0.1 --port 9491
    mongorestore --host 127.0.0.1 --port 9491 --nsInclude=BENCH_DB.BENCH_DB --archive --drop
    ```

* InfluxDB:
    ```console
    influxd backup -portable -db BENCH_DB -host 127.0.0.1:9492 InfluxDB_BENCH_DB
    influxd restore -portable -db BENCH_DB -host 127.0.0.1:9492 InfluxDB_BENCH_DB
    ```
</details>