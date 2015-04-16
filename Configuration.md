## Loading Order ##

ParaMark loads configuration files from following paths _in following order_:
  1. ParaMark internal hard-coded default configuration
    * Use "--print-default-conf" option to print out this file
    * Or refer to `PARAMARK_DEFAULT_CONFIG_STRING` in [opts.py](http://code.google.com/p/paramark/source/browse/fs/opts.py)
  1. `.paramark.conf` in user home directory, or `~/.paramark.conf`, if exists
  1. `.paramark.conf` in current working directory, if exists
  1. Configuration file specified by "-c" or "--conf" option
  1. Command options, if specified
**IMPORTANT NOTE**: Options that comes later _override_ former ones.

## Prepare a Configuration File ##
  1. Put a configuration template (e.g. following [Example](Example.md)) in the place according to required loading order, using
```
$ fsbench --print-default-conf > your_paramark_config_file
```
  1. Modify the corresponding value

## Example ##
```
# ParaMark Default Benchmarking Configuration
# last updated: 2010/08/10

##########################################################################
# Howto:
#   * Only modify the values you would like to change.
#   * Lines beginning with '#' or ';' are ignored.
#   * Following the conventions of this file will be safe.
##########################################################################

##########################################################################
# Global Options
##########################################################################
[global]
# Benchmark working directory
wdir = ./

# Number of concurrent benchmarking thread
nthreads = 1

# Ask user whether to proceed on critical situations
confirm = True

# Verbosity level (0-5)
verbosity = 0

# Log directory of benchmarking results
# Generate a random log directory when logdir is not set
logdir =

# Metadata operations to be performed
# Does not support line continuation now, keep option in one line
# e.g.,
# meta = mkdir,rmdir,creat,access,open,open_close,stat_exist,stat_non,utime,chmod,rename,unlink
meta = 

# I/O operations to be performed
# e.g., 
# io = read,reread,write,rewrite,fread,freread,fwrite,frewrite
io = 

# Overwrite following local settings
override = True

# Variables to override
opcnt = 10
factor = 16

# File size and block size
# e.g., fsize=1K,2M,3G, bsize=1KB,2mb,3gb
fsize = 1M
bsize = 1K

# Report configuration

##########################################################################
# Local Operation Options
#   * Safe to leave alone
#   * Each operation in a seperate section
##########################################################################

#
# Options for flags
# O_RDONLY, O_WRONLY, RDWR, O_APPEND, O_CREAT, O_EXCL
# O_TRUNC or their inclusive OR
#
# Options for mode
# S_ISUID, S_ISGID, S_ENFMT, S_ISVTX, S_IREAD,
# S_IWRITE, S_IEXEC, S_IRWXU, S_IRUSR, S_IWUSR,
# S_IXUSR, S_IRWXG, S_IRGRP, S_IWGRP, S_IXGRP,
# S_IRWXO, S_IROTH, S_IWOTH, S_IXOTH or their bitwise OR
#

# Metadata operation
[mkdir]
opcnt = 0
factor = 16

[rmdir]
opcnt = 0
factor = 16

[creat]
opcnt = 0
factor = 16
flags = O_CREAT | O_WRONLY | O_TRUNC 
mode = S_IRUSR | S_IWUSR

[access]
opcnt = 0
# F_OK, R_OK, W_OK, X_OK or their inclusive OR
factor = 16
mode = F_OK

[open]
opcnt = 0
factor = 16
flags = O_RDONLY
mode = S_IRUSR

[open_close]
opcnt = 0
factor = 16
flags = O_RDONLY
mode = S_IRUSR

[stat_exist]
opcnt = 0
factor = 16

[stat_non]
opcnt = 0
factor = 16

[utime]
opcnt = 0
factor = 16
times =

[chmod]
opcnt = 0
factor = 16
mode = S_IEXEC

[rename]
opcnt = 0
factor = 16

[unlink]
opcnt = 0
factor = 16

# I/O operation
[read]
fsize = 0
bsize = 0
flags = O_RDONLY
mode = S_IRUSR

[reread]
fsize = 0
bsize = 0
flags = O_RDONLY
mode = S_IRUSR

[write]
fsize = 0
bsize = 0
flags = O_CREAT | O_RDWR
mode = S_IRUSR | S_IWUSR
fsync = False

[rewrite] 
fsize = 0
bsize = 0
flags = O_CREAT | O_RDWR
mode = S_IRUSR | S_IWUSR
fsync = False

[fread]
fsize = 0
bsize = 0
# 'r', 'w', 'a', 'b', '+', or their combinations
mode = r
bufsize = 

[freread]
fsize = 0
bsize = 0
mode = r
bufsize = 

[fwrite]
fsize = 0
bsize = 0
mode = w
bufsize = 
fsync = False

[frewrite]
fsize = 0
bsize = 0
mode = w
bufsize =
fsync = False
```


---

_If you have any questions, please leave a comment in below suggestion box._