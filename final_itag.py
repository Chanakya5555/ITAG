import psycopg2
from io import StringIO
from datetime import datetime
import os
import time
from tqdm import tqdm 


conn = psycopg2.connect(
    dbname="assignment", user="postgres", password="xyzzyspoon", host="localhost"
)
cur = conn.cursor()


file_path = r"E:\Assignment\022_20250810202034.ITAG"
log_file_path = "ingestion_error.log"
log_buffer = []


header_record_count = 0
header_status_counts = {"1":0,"2":0,"3":0,"4":0}
actual_record_count = 0
actual_status_counts = {"1":0,"2":0,"3":0,"4":0}
timestamp_now = datetime.now().isoformat(sep=" ")
batch_size = 100000
batch_buffer = StringIO()

total_bytes = os.path.getsize(file_path)

start_time = time.time()

def log_error(line_num, reason, content=""):
    """Error info to log buffer."""
    log_buffer.append(f"Line {line_num}: {reason} | Content: {content}\n")

with open(file_path, "r", encoding="ascii") as f:
    

    header = f.readline()
    if len(header) < 61:
        log_error(1, "Header too short", header)
    else:
        file_type = header[0:4]
        from_agency_id = header[4:7]
        file_date = header[7:15] 
        file_time = header[15:21] 
        record_count_str = header[21:29]
        count_stat1_str = header[29:37]
        count_stat2_str = header[37:45]
        count_stat3_str = header[45:53]
        count_stat4_str = header[53:61]

        # Validate header fields
        header_valid = True
        if file_type != "ITAG":
            log_error(1, f"Invalid FILE_TYPE: {file_type}", header)
            header_valid = False
        if not from_agency_id.isdigit():
            log_error(1, f"FROM_AGENCY_ID not numeric: {from_agency_id}", header)
            header_valid = False
        try:
            datetime.strptime(file_date, "%Y%m%d")
        except ValueError:
            log_error(1, f"Invalid FILE_DATE: {file_date}", header)
            header_valid = False
        try:
            datetime.strptime(file_time, "%H%M%S")
        except ValueError:
            log_error(1, f"Invalid FILE_TIME: {file_time}", header)
            header_valid = False
        if not record_count_str.isdigit():
            log_error(1, f"RECORD_COUNT not numeric: {record_count_str}", header)
            header_valid = False
        header_record_count = int(record_count_str) if record_count_str.isdigit() else 0

        for i, val in enumerate([count_stat1_str, count_stat2_str, count_stat3_str, count_stat4_str], start=1):
            if not val.isdigit():
                log_error(1, f"COUNT_STAT{i} not numeric: {val}", header)
                header_valid = False
        header_status_counts["1"] = int(count_stat1_str) if count_stat1_str.isdigit() else 0
        header_status_counts["2"] = int(count_stat2_str) if count_stat2_str.isdigit() else 0
        header_status_counts["3"] = int(count_stat3_str) if count_stat3_str.isdigit() else 0
        header_status_counts["4"] = int(count_stat4_str) if count_stat4_str.isdigit() else 0

        if header_valid:
            print(f"Header validated successfully: File Type={file_type}, Agency={from_agency_id}, Date={file_date}, Time={file_time}, Record Count={header_record_count}")

    # Detail records
    bytes_read = len(header)
    with tqdm(total=total_bytes, unit="B", unit_scale=True, desc="Processing ITAG file") as pbar:
        pbar.update(bytes_read)

        for line_num, line in enumerate(f, start=2):
            line_len = len(line)
            bytes_read += line_len
            pbar.update(line_len)

            line = line.rstrip("\r\n")
            if len(line) < 17:
                log_error(line_num, f"Line too short: {len(line)}", line)
                continue

            tag_agency_id = line[0:3]
            tag_serial_number = line[3:16]
            tag_status = line[16]
            tag_acct_info = line[17:] if len(line) > 17 else None

            # Validate detail fields
            invalid_reason = []
            if not tag_agency_id.isdigit():
                invalid_reason.append(f"TAG_AGENCY_ID not numeric: {tag_agency_id}")
            if not tag_serial_number.isdigit():
                invalid_reason.append(f"TAG_SERIAL_NUMBER not numeric: {tag_serial_number}")
            if tag_status not in ("1","2","3","4"):
                invalid_reason.append(f"Invalid TAG_STATUS: {tag_status}")

            if invalid_reason:
                log_error(line_num, "; ".join(invalid_reason), line)
                continue

            actual_record_count += 1
            actual_status_counts[tag_status] += 1
            batch_buffer.write(f"{tag_agency_id},{tag_serial_number},{tag_status},{timestamp_now}\n")

            # Batch COPY
            if actual_record_count % batch_size == 0:
                batch_buffer.seek(0)
                cur.copy_from(batch_buffer, "itag_tag_status", sep=",",
                              columns=("tag_agency_id","tag_serial_number","tag_status","processed_at"))
                conn.commit()
                batch_buffer = StringIO()


batch_buffer.seek(0)
cur.copy_from(batch_buffer, "itag_tag_status", sep=",",
              columns=("tag_agency_id","tag_serial_number","tag_status","processed_at"))
conn.commit()


if actual_record_count != header_record_count:
    log_error("END", f"RECORD_COUNT mismatch: expected {header_record_count}, got {actual_record_count}")

for status in ("1","2","3","4"):
    if actual_status_counts[status] != header_status_counts[status]:
        log_error("END", f"COUNT_STAT{status} mismatch: expected {header_status_counts[status]}, got {actual_status_counts[status]}")


if log_buffer:
    with open(log_file_path, "a", encoding="utf-8") as log_file:
        log_file.writelines(log_buffer)

cur.close()
conn.close()


end_time = time.time()
elapsed_time = end_time - start_time

print(f"\nITAG ingestion complete. Records processed: {actual_record_count}, Errors: {len(log_buffer)}")
print(f"Total runtime: {elapsed_time:.2f} seconds")