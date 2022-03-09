import os
import logging
import json
import re
import gzip
import argparse
import io
import traceback
from datetime import datetime
from collections import namedtuple
from string import Template


LOG_RECORD_RE = re.compile(
    '^'
    '\S+ '  # remote_addr
    '\S+\s+'  # remote_user (note: ends with double space)
    '\S+ '  # http_x_real_ip
    '\[\S+ \S+\] '  # time_local [datetime tz] i.e. [29/Jun/2017:10:46:03 +0300]
    '"\S+ (?P<href>\S+) \S+" '  # request "method href proto" i.e. "GET /api/v2/banner/23815685 HTTP/1.1"
    '\d+ '  # status
    '\d+ '  # body_bytes_sent
    '"\S+" '  # http_referer
    '".*" '  # http_user_agent
    '"\S+" '  # http_x_forwarded_for
    '"\S+" '  # http_X_REQUEST_ID
    '"\S+" '  # http_X_RB_USER
    '(?P<time>\d+\.\d+)'  # request_time
)

DateNamedFileInfo = namedtuple('DateNamedFileInfo', ['file_path', 'file_date'])

DEFAULT_CONFIG_PATH = REPORT_TEMPLATE_PATH = os.path.dirname(os.path.abspath(__file__))


def load_conf(conf_path):
    with open(conf_path, 'rb') as conf_file:
        conf = json.load(conf_file, encoding='utf8')
    return conf


####################################
# Analyzing
####################################


def create_report(records, max_records):
    total_records = 0
    total_time = 0
    intermediate_data = {}

    for href, response_time in records:
        total_records += 1
        total_time += response_time
        # CODE HERE


def get_log_records(log_path, errors_limit=None):
    open_fn = gzip.open if is_gzip_file(log_path) else io.open
    errors = 0
    records = 0
    log_records = []
    with open_fn(log_path, mode='rb') as log_file:
        for log_line in log_file.readlines():
            log_records.append(parse_log_record(log_line))
        # CODE HERE

    if errors_limit is not None and records > 0 and errors / float(records) > errors_limit:
        raise RuntimeError('Errors limit exceeded')

    return log_records


def parse_log_record(log_line):
    # CODE HERE
    str_log_line = log_line.decode("utf-8")
    href = re.findall(r'"\S+ (?P<href>\S+) \S+" ', str_log_line)[0]
    request_time = re.findall(r'(?P<time>\d+\.\d+)', str_log_line)[0]
    return href, request_time


def median(values_list):
    if not values_list:
        return None

    # CODE HERE


####################################
# Utils
####################################

def setup_logger(log_path):
    log_dir = os.path.split(log_path)[0]
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(filename=log_path, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')


def get_latest_log_info(files_dir):
    if not os.path.isdir(files_dir):
        return None

    latest_file_info = None
    for filename in os.listdir(files_dir):
        match = re.match(r'^nginx-access-ui\.log-(?P<date>\d{8})(\.gz)?$', filename)
        if not match:
            continue
        # CODE HERE
    return latest_file_info


def is_gzip_file(file_path):
    return file_path.split('.')[-1] == 'gz'


def render_template(template_path, to, data):
    if data is None:
        data = []
    # CODE HERE


def main(config):
    # resolving an actual log
    latest_log_info = get_latest_log_info(config['LOGS_DIR'])
    if not latest_log_info:
        logging.info('Ooops. No log files yet')
        return

    report_date_string = latest_log_info.file_date.strftime("%Y.%m.%d")
    report_filename = "report-{}.html".format(report_date_string)
    report_file_path = os.path.join(config['REPORTS_DIR'], report_filename)

    if os.path.isfile(report_file_path):
        logging.info("Looks like everything is up-to-date")
        return

    # report creation
    logging.info('Collecting data from "{}"'.format(os.path.normpath(latest_log_info.file_path)))
    log_records = get_log_records(latest_log_info.file_path, config.get('ERRORS_LIMIT'))
    report_data = create_report(log_records, config['MAX_REPORT_SIZE'])

    render_template(REPORT_TEMPLATE_PATH, report_file_path, report_data)

    logging.info('Report saved to {}'.format(os.path.normpath(report_file_path)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Config file path', default=DEFAULT_CONFIG_PATH)
    args = parser.parse_args()
    try:
        config = load_conf(args.config)
    except (json.decoder.JSONDecodeError, FileNotFoundError):
        config = {
            "REPORT_SIZE": 1000,
            "REPORT_DIR": "./reports",
            "LOG_DIR": "./log"
        }
    setup_logger(config.get('LOG_FILE'))

    try:
        main(config)
    except Exception as exc:
        logging.exception(traceback.print_tb(exc.__traceback__))
