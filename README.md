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
* [Installation Wiki]&#40;https://gitlab.com/lvn2007/XStore/-/wikis/installation&#41;

# How to Use
* [XStore API]&#40;https://gitlab.com/lvn2007/XStore/-/wikis/api&#41;
* [Data Structure]&#40;https://gitlab.com/lvn2007/XStore/-/wikis/Data%20Structure&#41;


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
