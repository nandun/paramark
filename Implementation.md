

# Data #

## SQLite Database Format ##

ParaMark uses [sqlite3](http://www.sqlite.org/) to store and retrieve benchmark data,
which makes the trace data portable and queryable by SQL commands or APIs.

### Runtime Table ###

| _Column_ | **item** | **value** |
|:---------|:---------|:----------|
| _Type_ | TEXT | TEXT |
| _Description_| item name | value string |

### Operation Table ###

For each filesystem operation, there is a table corresponded to the operation and named as the operation name. This is useful for aggregating and comparing between different configurations of the same operation.

An I/O operation has following table format:

| _Column_ | **hid** | **pid** | **tid** | **fsize** | **bsize** | **elapsed** | **agg** | **opavg** | **opmin** | **opmax** |
|:---------|:--------|:--------|:--------|:----------|:----------|:------------|:--------|:----------|:----------|:----------|
| _Type_ | INTEGER | INTEGER | INTEGER | INTEGER | INTEGER | BLOB | REAL | REAL | REAL | REAL |
| _Description_ | host ID | process ID | thread ID | file size | block size | list of elapsed time | aggregated throughput | average operation throughput | min operation throughput | max operation throughput |

A metadata operation has following table format:
| _Column_ | **hid** | **pid** | **tid** | **count** | **factor** | **elapsed** | **agg** | **opavg** | **opmin** | **opmax** |
|:---------|:--------|:--------|:--------|:----------|:-----------|:------------|:--------|:----------|:----------|:----------|
| _Type_ | INTEGER | INTEGER | INTEGER | INTEGER | INTEGER | BLOB | REAL | REAL | REAL | REAL |
| _Description_ | host ID | process ID | thread ID | operation count | directory tree factor | list of elapsed time | aggregated throughput | average operation throughput | min operation throughput | max operation throughput |