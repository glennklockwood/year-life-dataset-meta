This dataset contains the Darshan logs from which the feature vectors generated
for "A Year in the Life of a Parallel File System" by Lockwood et al, presented
at the 2018 International Conference for High Performance Computing, Networking,
Storage, and Analysis (SC'18), were derived.  The Darshan logs are stored in the
binary format dictated by Darshan 3.1 as generated on the Cori, Edison, and Mira
systems.

This dataset also includes the feature vectors used in the aforementioned paper,
encoded in CSV format.  **These feature vectors are identical to those also
included in the tokio-abcutils code repository that contains all analysis and
figures presented.**  They are simply duplicated here for convenience.

This repository contains the following:

* README.md - This file
* LICENSE.md - The license under which these data have been published
* INDEX.csv - An index of all Darshan logs included in this dataset.  It contains
    1. `log_file`: the name of each Darshan log
    2. `date`: The date on which the job was run and the log was generated
    3. `compute_system`: Whether the job was run on Edison, Cori, or Mira
    4. `file_system`: The file system on which the job was run.  Required to
       disambiguate jobs run on Edison, which has three file systems.
    5. `application`: The name of the benchmark application used to generate the
       data.  IOR and HACC benchmarks are named accordingly; `dbscan_read` is
       BD-CATS and `vpicio_uni` is VPIC.
    6. `shared_or_fpp`: Whether the job generated file-per-process or
       single-shared-file I/O.  Required to disambiguate IOR jobs which were run
       in both modes.
    7. `read_or_write`: Whether the job predominantly performed reads or writes.
       Requires to disambiguate IOR jobs which were run in both modes.
    8. `md5`: The MD5 digest for the Darshan log.
* darshan\_logs/ - Directory containing actual Darshan logs.  Logs are sorted
  into subdirectories according to the year and month in which they were
  generated.
* summaries/ - Directory containing the feature vector CSVs that fed into the
  plots presented in the paper.  Identical to the eponymous files in the
  [tokio-abcutils release][]'s `summaries` subdirectory.
* index\_darshan\_logs.py - The script used to generate `INDEX.csv` from the
  individual Darshan logs.  Useful to determine exactly how the columns in
  INDEX.csv map to the raw data in each log.
* organize\_logs\_by\_date.py - The script used to collect the Darshan logs into
  date-indexed subdirectories.  Provided for provenance.

[tokio-abcutils release]: https://dx.doi.org/10.5281/zenodo.1345786
