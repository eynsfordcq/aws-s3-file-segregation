# S3 File Segregation Tool

## Overview
The S3 File Segregation script is used to segregate files stored in Amazon S3 based on date extracted from their filenames. This script utilizes the `boto3` library to interact with AWS S3 to move files into designated directories based on date patterns. 

Key features include:
- Multi-threaded processing for performance optimization using `ThreadPoolExecutor`.
- Error handling that logs issues encountered during file processing.
- Flexible configuration to accommodate various use cases.

Note: This script assumes that AWS CLI is configured and has all necessary permissions to interact with S3. 
[Guide to AWS CLI configuration](https://gitlab.globeoss.com/dst/dst_aws/general/-/issues/3)


## Installation
Ensure you have Python 3.11 installed on your system. Clone the repository:
```bash
git clone https://<repository-url>
```

Install required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Copy the `config.xml.example` and configure it accordingly.

| Parameter         | Required | Type   | Permission | Description                                                                                     |
| ----------------- | -------- | ------ | ---------- | ----------------------------------------------------------------------------------------------- |
| log_file          | true     | file   | write      | filepath to write log                                                                           |
| match_pattern     | false    | string | -          | regex pattern to match datestring from filename, use groups to indicate the datestring          |
| datetime_format   | true     | string | -          | strftime format.                                                                                |
| time_delay        | false    | int    | -          | time delay in seconds, defaults to 86400.                                                       |
| s3_dir            | true     | string | -          | s3 directory to read.                                                                           |
| s3_segregated_dir | true     | string | -          | s3 directory to store segregated files. supports strftime format                                |
| s3_error_dir      | true     | string | -          | s3 directory to store files that fail to match date pattern.                                    |
| n_keys            | false    | int    | -          | number of keys to list per call to s3.list_objects_v2, max 1000 and defaults to 500.            |
| n_loops           | false    | int    | -          | maximum number of loops the program can run. this is to prevent infinite loop. defaults to  10. |
| n_workers         | false    | int    | -          | number of threads to spawn concurrently. defaults to half of system's cpu count.                |

- `match_pattern` is a regex, put the datestring in groups `()`, the program concat all the groups together.
    - for example, for a file as such: `CHARGINGCDR_4008-BRCCNH-CCNCDR44-01-Blk0Blk-8421-20221231-100511-78.ccn`    
    - set the match_pattern as `"^CHARGINGCDR_.*-(\d{8})-?(\d{2}).*"`
    - match group 0 is `20221231` and match group 1 is `10`
    - the program concat them together to `2022123110`
    - therefore, the datetime format should be `%Y%m%d%H`
- if `match_pattern` is not provided, the program assumes the date is `today's date - time_delay`
- the number of files the script can segregate is determined by `(n_keys - 1) * n_loops`. 
    - the one key reserved is for the directory itself and will be ignored by the script
    - for example using default settings `n_keys=500` and `n_loops=10`, the script can segregate 4,990 files per run.
    - set n_loops higher if the number of files has high variance, it's just there to prevent infinite loop if for some reason a file cannot be moved, the script terminates when there are no more files.

## Usage
To run the S3 File Segregation Tool, use the following command:

```bash
usage: main.py [-h] --config CONFIG [--datetime DATETIME] [--verbose]

options:
  -h, --help            show this help message and exit
  --config CONFIG, -c CONFIG
                        XML config file
  --datetime DATETIME, -d DATETIME
                        Specify the date that current files will move into
  --verbose, -v         Detail log

```

## Tests
To run the test script, use the following command:

```bash
python -m pytest -s -vv tests/
```

> The test covers only datetime extraction and does not cover the other part like S3 operations.


