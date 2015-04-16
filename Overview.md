# Table of Contents #
> <a href='Hidden comment: Always copy from TableOfCotents page'></a>
  * [Overview](Overview.md)
  * Benchmarking Your File System
    * [Usage](Usage.md)
    * [Configuration](Configuration.md)
    * [Examples](Examples.md)
  * Extending ParaMark
    * [Reference](Reference.md)
    * [Implementation](Implementation.md)

# Introduction #
ParaMark is a fine-grained, easy-to-use, easy-to-understand benchmark for parallel/distributed systems.

  * Written in Python, runs everywhere
  * System call level fine-grained performance logging
  * Easy-reading HTML benchmark report allows to investigate thread-level to system-level performance
  * Integrated with GXP parallel shell, easy to deploy and use in large-scale distributed environments

# Future Work #

  * Representative parallell workload
  * File system image synthesis

# References #

  * File and Storage System Benchmarking Portal: http://fsbench.filesystems.org/

  * IOzone Filesystem Benchmark: http://www.iozone.org/

  * Bonnie++ Filesystem Benchmark: http://sourceforge.net/projects/bonnie/

  * Pianola: A script-based I/O benchmark ([pdf](http://www.pdsi-scidac.org/events/PDSW08/resources/papers/pianola_pdsi.pdf))

  * Nitin Agrawal, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. Generating Realistic Impressions for File-System Benchmarking. _Proceedings of the 7th Conference on File and Storage Technologies_ (FAST '09), Feb 2009, San Francisco, CA. [PDF](http://www.usenix.org/events/fast09/tech/full_papers/agrawal/agrawal.pdf)

  * Keith A. Smith and Margo I. Seltzer. File system aging -- Increasing the relevance of file system benchmarks. _Proceedings of the 1997 SIGMETRICS Conference_, 1997, Seattle, WA. [PDF](http://www.eecs.harvard.edu/~margo/papers/sigmetrics97-fs/paper.pdf)