# Install and Preparation #

The minimum requirement to run ParaMark is [Python](http://www.python.org/download/) (version >= 2.5.2). You only need to perform _Step 2_ on one single machine where you can produce benchmark reports and charts.

  1. Get the source code
    * To get the latest version by following the instructions on the [Source Checkout](http://code.google.com/p/paramark/source/checkout) page, Or just typing
```
$ hg clone https://paramark.googlecode.com/hg/ paramark
```
    * To get stable release from the project [Downloads](http://code.google.com/p/paramark/downloads/list) page, if exists. Then typing
```
$ tar zxf paramark-X.Y.tgz
```
  1. Install required packages
    * For Debian/Ubuntu systems, just execute following script in ParaMark source directory.
```
$ sudo ./prepare.sh
```
    * For other distributions of Linux, you have to manually install following packages by yourself: [Python](http://www.python.org), [SciPy](http://www.scipy.org) [Gnuplot.py](http://gnuplot-py.sourceforge.net/) and [Gnuplot](http://www.gnuplot.info/).

# Benchmarking #
ParaMark can be used as a standalone file system benchmark like other ones, such
as [iozone](http://www.iozone.org/), [bonnie++](http://www.coker.com.au/bonnie++/), etc.
It can be also used to benchmark clusters, Grids, or supercomputers, where it uses [GXP shell](http://www.logos.ic.i.u-tokyo.ac.jp/gxp/) as its backend for message passing and synchronization.

## Standalone Mode ##

Simply run
```
$ ./fsbench
```
will give you a quick impression of standalone benchmarking using default configurations.

For more details, typing
```
$ fsbench -h
```

## GXP Mode ##
To run fsbench in GXP mode (parallel mode), you have to install and learn how to invoke GXP shell first, please refer to [GXP User's Guide](http://www.logos.t.u-tokyo.ac.jp/gxp/stuff/doc/gxpman.html).

After exploring target nodes, simply run
```
$ gxpc mw fsbench -g [options]
```

**IMPORTANT NOTE**:
  * Don't forget "`-g`" to indicate GXP mode, or `fsbench` will be executed independently on each node. It is recommended to set shell alias for your convenience as
```
$ alias short_cmd="gxpc mw fsbench -g"
```
  * It is recommended to specify configuration file by "-c" option, otherwise every node may load different configurations according to their own search paths.

## Configuration ##
It is always burden and error-prone for users if using command line to specify many
benchmarking parameters. Therefore, ParaMark _only_ uses configuration files instead of command line
options to make benchmarking practice easy, precise, and flexible.

ParaMark searches default paths for configuration file or use the one specified by users using "`-c`" option.

For the details of ParaMark configuration file, please refer to section [Configuration](Configuration.md).

**NOTE**:
A few options are still available for convenience. Please refer to "`fsbench -h`", where command options have highest priority and will override any loaded configurations.

# Results Reporting #

ParaMark perform following procedures to automatically generate the benchmarking report in various formats for users.
  1. Store the runtime log data to sqlite-compatible database, in memory (temporarily) or disk (persistently).
  1. Parse and aggregate the performance data
  1. Generate performance report

ParaMark always generate an HTML report with rich information of benchmarking results by default, and you can view this report by most popular browsers. However, you can use different options to specify the formats. For more details, please refer to "`fsbench -h`".

**TIPS**:
  * `-n, --no-report` option: Tells ParaMark does not generate report after benchmarking, only store the data. This is useful when: 1) The benchmarking target does not have required software packages to generate charts, then you can copy the data to the place where it can be processed; 2) To save the report generation time if your time slot is precious. Then you can use `-r` to specify the directory where data is stored.
  * `--text-report` or `--csv-report`: For those who do not need beautiful HTMLs.
  * User can produce their own report by using the raw data from benchmarking database. It is usually stored in the log directory as `fsbench.db` in standard sqlite format. For the specification of the database, please refer to section [Implementation](Implementation#Data.md).
  * `-q, --quick-report` option: Do not output database to file system, instead in memory, and produce text report. This is useful when you only care about the whole aggregated performance and would like to know the results as soon as possible.


---

_If you have any questions, please leave a comment in below suggestion box._