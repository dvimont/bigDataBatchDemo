This package provides prototype batch functionality to
(1) download raw, hourly "pageview" data files** from the WikiMedia Foundation's website
    (via execution of the WikimediaFileDownloader#downloadRawDataFiles method);
(2) run several MapReduce jobs to consolidate daily, weekly, monthly, and yearly pageview stats for each webpage
    (see script runPageViewMapReduceJobs in project's root directory).

ADDITIONALLY, the project contains some experimental Protocol Buffer implementations for
WikiMedia pageview objects. Prototype code in the MasterDatasetLoader class uses the protobuf
structures to load 0-terminated records into an HDFS file. When difficulties were encountered
in inputting this file into a MapReduce stream, it was decided that Twitter's elephantbird
infrastructure is likely required to facilitate this; and a simpler text-based approach (not
utilizing protobufs) was ultimately utilized in the MapReduce jobs presented in this package.

ALSO, in an earlier phase of experimentation, the WikiMediaFileConsolidator class was
written to decompress and consolidate all raw data files into a single, HDFS-splittable file.

** Note that downloading ALL available compressed "pageview" data files from WikiMedia might require
approximately 400GB to 500GB of storage.
