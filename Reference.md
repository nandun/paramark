

# File System Benchmark #

## fsop -- File System Operation Primitives ##

_class_ `fsop`.**MetaOp**(name, files, `[`verbose`[`, dryrun`]]`)

## fsload -- File System Load Generation ##

_class_ `fsload`.**MetaLoad**()

_class_ `fsload`.**IOLoad**()

## fsbench -- File System Benchmark ##

_class_ `fsbench`.**Bench**()

# Benchmark Data Persistence #

## Class DataTable ##

DataTable.**setName(**_tableName_**)**

> Set the name of data table. Each data table is identified by its name.

DataTable.**csv2table(**_src_**)**

> Convert the csv file named _src_ to data table, previous data will be overwritten.
> Thus, this method is usually used when importing csv to a newly created data table.

# Results Analysis and Report #