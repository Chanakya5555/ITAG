# ITAG File Ingestion Program

## Overview

This program validates and ingests a provided ITAG file into a
PostgreSQL database.\
It performs the following tasks:

-   Validates the header record (file type, agency ID, date/time,
    counts).
-   Validates each detail record (agency ID, serial number, tag status).
-   Logs any errors with line numbers and reasons.
-   Loads valid records into the database table `itag_tag_status` using
    batch inserts for high performance.


## How to Run

### 1. Create target table in PostgreSQL

``` sql
CREATE TABLE itag_tag_status (
    tag_agency_id VARCHAR(10),
    tag_serial_number VARCHAR(20),
    tag_status SMALLINT,
    processed_at TIMESTAMP
);
```

### 2. Update DB connection settings inside the script

``` python
conn = psycopg2.connect(
    dbname="test3", 
    user="postgres", 
    password="xyzzyspoon", 
    host="localhost"
)
```

### 3. Set the ITAG file path

``` python
file_path = r"E:\itag_assignment\022_20250810202034.ITAG"
```

### 4. Run the script

``` bash
python itag_ingest.py
```

### 5. Check results

-   Inserted records → `itag_tag_status` table\
-   Errors → `itag_ingestion_errors.log`\
-   Console → Progress bar + runtime summary


## Performance Testing

**Initial version** - Used per-record validation and direct inserts into
the database. - Each error was written to the log file immediately as it
was encountered. - As a result, a large ITAG file took \~21 minutes to
complete.

**Optimized version (current)** - Database inserts are handled in large
batches using PostgreSQL's `COPY FROM`. - Errors are collected in memory
first, then flushed to the log file once at the end. - With these
changes, the same ITAG file now completes ingestion in about **80
seconds**.

This shift from record-by-record processing to batch-oriented ingestion
drastically improved performance and made the program suitable for
production-scale files.


## Logging Improvements

**Initially** - The log file was written line by line during
processing. - Frequent file writes slowed down overall execution.

**Now** - Errors are buffered in memory during validation. - The
complete error log is written at the end of execution in one go. - The
log file still contains the same details (line number + reason +
content), but it is produced much faster.

### Example Log Entries

``` txt
Line 5: Invalid TAG_STATUS | Content: 0211800008710000009
Line 12: Invalid length 16, expected 18 | Content: 02112000091300000
Line END: RECORD_COUNT mismatch: expected 1000000, got 999998
Line END: COUNT_STAT2 mismatch: expected 250000, got 249998
```

Lines with actual errors (wrong tag status, wrong length) are recorded
with line numbers.\
Final "END" entries indicate header vs actual count mismatches.


## Assumptions

-   Date format in the header is assumed to be **YYYYMMDD**.
-   Anything after the 17th character in a detail record is treated as
    `tag_acc_info`.
-   Leading zeros in `tag_agency_id` and `tag_serial_number` must be
    preserved → therefore stored as **VARCHAR** in DB.
-   Invalid records are skipped but logged; only valid records are
    inserted.
