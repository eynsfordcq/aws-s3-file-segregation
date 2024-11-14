import os
import re 
import boto3
import argparse
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, wait

# suppress logs by boto3
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

def setup_logger(log_id: str, log_file, log_level=logging.INFO):
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))
    
    logging.basicConfig(
        filename=log_file, 
        level=log_level, 
        format=f'%(asctime)-15s [%(levelname)s][{log_id}][%(filename)s/%(funcName)s/%(lineno)d] - %(message)s'
    )

def parse_config_element(element):
    if len(element) == 0:
        return element.text
    else:
        child_dict = {}
        for child in element:
            # It doesn't check if it's in the dict_config
            # It will simply overwrite the existing key with the latest value
            child_dict[child.tag] = parse_config_element(child)
        return child_dict

def parse_config(config_path):
    dict_config = {}
    root = ET.parse(config_path).getroot()
    for child in root:
        dict_config[child.tag] = parse_config_element(child)
    return dict_config

def validate_args(args: argparse.Namespace) -> None:
    """Validates the provided command-line arguments.

    :args:
        args (argparse.Namespace): The parsed command-line arguments.

    :raises:
        FileNotFoundError: If the config file does not exist.
        PermissionError: If the config file has no read permission.
        ValueError: If the provided datetime is not a valid date

    :returns:
        None
    """
    # validate config
    if not os.path.exists(args.config):
        raise FileNotFoundError(f"Config file: {args.config} does not exist")
    
    if not os.access(args.config, os.R_OK):
        raise PermissionError(f"Config file: {args.config} has no read permission")
    
    # validate date args
    if args.datetime:
        try:
            datetime.strptime(args.datetime, "%Y-%m-%d %H:%M:%S")
        except:
            raise ValueError(f"Date argument: {args.datetime} not a valid date")

def validate_refine_config(dict_config: dict, args) -> dict:
    """
    validate and refine config file and args 
    """
    # process args 
    # args boolean
    args_bool_keys = ['verbose']
    for key in args_bool_keys:
        _key = f"args_{key}"
        try:
            dict_config[_key] = getattr(args, key)
        except:
            dict_config[_key] = False
    
    # other args (logging purpose)
    dict_config['args_datetime'] = args.datetime 
    dict_config['args_config'] = args.config

    config_keys = {
        # log_file
        # match_pattern
        # datetime_format
        # s3_dir
        # s3_segregated_dir
        # s3_error_dir
        "log_file": {
            "required": True,
            "type": "file",
            "permission": "write",
        },
        "match_pattern": {
            "required": False,
            "type": "str"
        },
        "datetime_format": {
            "required": True,
            "type": "str"
        },      
        "time_delay": {
            "required": False,
            "type": "int"
        },
        "s3_dir": {
            "required": True,
            "type": "str",
        },
        "s3_segregated_dir": {
            "required": True,
            "type": "str",
        },
        "s3_error_dir": {
            "required": True,
            "type": "str",
        },
        "n_keys": {
            "required": False,
            "type": "int"
        },
        "n_loops": {
            "required": False,
            "type": "int"
        },
        "n_workers": {
            "required": False,
            "type": "int"
        },
    }

    for key, setting in config_keys.items():
        _required = setting.get('required')
        _type = setting.get('type')
        _permission = setting.get('permission')
        _key_setting = (f"validate_config_error: key: {key}, "
                      f"required: {_required}, "
                      f"type: {_type}, "
                      f"permission: {_permission}, ")
        
        # check required keys
        if _required:
            if key not in dict_config:
                raise ValueError(
                    f"{_key_setting}"
                    f"error: missing required key."
                )
        
        # check directory
        if _type in ["directory", "file", "executable"] and dict_config.get(key):
            if _type == 'file' or key == 'raw_files_dir':
                _pathname = os.path.dirname(dict_config.get(key))
            else:
                _pathname = dict_config.get(key)
                
            if not os.path.exists(_pathname):
                raise ValueError(
                    f"{_key_setting}"
                    f"error: file/directory {_pathname} doesn't exist."
                )
            
            if _permission: 
                if _permission == 'write':
                    _permission_code = os.W_OK
                elif _permission == 'execute':
                    _permission_code = os.X_OK
                else:
                    _permission_code = os.R_OK
            
                if not os.access(_pathname, _permission_code):
                    raise ValueError(
                        f"{_key_setting}"
                        f"error: file/directory {_pathname} insufficient permission."
                    )

        # check int 
        if _type == 'int' and dict_config.get(key):
            try:
                dict_config[key] = int(dict_config[key])
            except: 
                raise ValueError(
                    f"{_key_setting}"
                    f"error: fail to convert key value '{dict_config[key]}' to int"
                )
        
        # check str 
        if _type == 'str' and dict_config.get(key):
            dict_config[key] = str(dict_config[key]).strip()

        # custom checks 
        if key == 'time_delay' and not dict_config.get(key): 
            dict_config[key] = 86400
        if key == 'n_keys' and not dict_config.get(key):
            dict_config[key] = 500
        if key == 'n_loops' and not dict_config.get(key):
            dict_config[key] = 10
        if key == 'n_workers' and not dict_config.get(key):
            dict_config[key] = os.cpu_count() // 2
    
    # args datetime 
    if not args.datetime:
        dict_config['process_date'] = datetime.now() - timedelta(seconds=dict_config['time_delay'])
    else:
        dict_config['process_date'] = datetime.strptime(args.datetime, "%Y-%m-%d %H:%M:%S")

    return dict_config

def parse_s3_uri(s3_path):
    s3_bucket, s3_key = s3_path.replace('s3://', '').split('/', 1)
    return s3_bucket, s3_key

def list_s3_get_files(
        s3_client, 
        s3_path, 
        n_keys: int = 1000, 
        n_loops: int = 10,
    ):

    s3_bucket, s3_directory = parse_s3_uri(s3_path)
    is_empty = False
    count_loop = 1
    while count_loop <= n_loops and not is_empty:
        # get s3 response
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket, 
            Prefix=s3_directory, 
            MaxKeys=n_keys
        )
    
        if not response.get('Contents'):
            is_empty = True 
            logging.warning(f"Empty set")
            break
            
        count = 0 
        paths = []
        for object in response['Contents']:
            _key = object.get("Key")
            # if directory (folder)
            if _key.endswith('/'):
                continue
            paths.append(os.path.join(s3_bucket, _key))
            count += 1
        
        if len(paths) == 0:
            is_empty = True 
            logging.warning(f"No files left")
            break
        
        logging.info(f"[curr_loop:{count_loop}][max_loops:{n_loops}][n_keys:{len(response['Contents'])}][n_file:{count}]")

        count_loop += 1
        yield paths

def move_files_to_s3(s3_client: boto3.client, source_path: str, destination_dir: str) -> None:
    try:
        source_bucket, source_key = parse_s3_uri(source_path)
        dest_bucket, dest_dir = parse_s3_uri(destination_dir)
        dest_key = os.path.join(dest_dir, os.path.basename(source_key))

        logging.debug(f"Moving {source_path} to {os.path.join(dest_bucket, dest_key)}")
        
        s3_client.copy_object(
            Bucket=dest_bucket, # destination bucket
            Key=dest_key, # destination key
            CopySource=source_path, # {bucket}/{key}
        )

        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
    
    except Exception as e:
        logging.error(f"Error moving {source_path} to {destination_dir}: {e}")

def extract_datetime_from_filename(filename, regex_pattern, datetime_format) -> datetime:
    pattern = re.compile(regex_pattern)
    match = pattern.search(filename)
    
    if not match:
        logging.error(
            f"no match found: "
            f"filename: {filename} "
            f"regex: {regex_pattern}"
        )
        return None
    
    # join groups together
    date_str = ''.join([ str(match) for match in match.groups() ])
    
    try:
        date_obj = datetime.strptime(date_str, datetime_format)
    except ValueError as e:
        logging.error(
            f"fail to convert date: "
            f"filename: {filename} "
            f"regex : {regex_pattern} "
            f"matched_date_str: {date_str} "
            f"datetime_format: {datetime_format} "
            f"error: {e}"
        )
        return None
    
    return date_obj

def start_segregation(s3_client, dict_config):
    s3_dir = dict_config.get("s3_dir")
    s3_segregated_dir = dict_config.get("s3_segregated_dir")
    s3_error_dir = dict_config.get("s3_error_dir")
    match_pattern = dict_config.get("match_pattern")
    datetime_format = dict_config.get("datetime_format")
    time_delay = dict_config.get("time_delay")
    n_keys = dict_config.get("n_keys")
    n_loops = dict_config.get("n_loops")
    n_workers = dict_config.get("n_workers")
    default_date = datetime.now() - timedelta(seconds=time_delay)
    
    try:
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            for files in list_s3_get_files(s3_client, s3_dir, n_keys=n_keys, n_loops=n_loops):
                futures = []
                for file in files:
                    if not match_pattern:
                        _date = default_date
                    else: 
                        _date = extract_datetime_from_filename(
                            os.path.basename(file),
                            match_pattern,
                            datetime_format
                        )

                    if not _date:
                        futures.append(executor.submit(move_files_to_s3, s3_client, file, s3_error_dir))
                    else:
                        _destination_dir = _date.strftime(s3_segregated_dir)
                        futures.append(executor.submit(move_files_to_s3, s3_client, file, _destination_dir))
                
                wait(futures)

    except Exception as e:
        logging.error(f"{e}")

if __name__ == '__main__':
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="XML config file", type=str, required=True)
    parser.add_argument("--datetime", "-d", help="Specify the date that current files will move into", type=str)
    parser.add_argument("--verbose", "-v", help="Detail log", action="store_true")
    args = parser.parse_args()
    validate_args(args)

    # parse config
    dict_config = parse_config(args.config)
    dict_config = validate_refine_config(dict_config, args)

    # setup logger
    log_path = datetime.now().strftime(dict_config['log_file'])

    setup_logger(
        log_id=dict_config['process_date'].strftime('s3_segregation_%Y%m%d'),
        log_file=log_path,
        log_level=logging.DEBUG if dict_config['args_verbose'] else logging.INFO
    )

    logging.info(f'Start seggregation for {dict_config["process_date"]}')
    logging.debug(f"dict_config: {dict_config}")
    
    s3_client = boto3.client('s3')
    start_segregation(s3_client, dict_config)
    
    logging.info(f"Finished file segregation for {dict_config['process_date'].strftime('%Y%m%d')}")
