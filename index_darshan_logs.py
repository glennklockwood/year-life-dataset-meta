#!/usr/bin/env python
"""
Parse and cache a Darshan log to simplify reanalysis and sharing its data.
"""

import os
import re
import json
import glob
import warnings
import datetime
import argparse
import multiprocessing
import hashlib
import pandas
import tokio.connectors.darshan

FSNAME_TO_SYSTEM = {
    "mira-fs1": "mira",
    "scratch1": "edison",
    "scratch2": "edison",
    "scratch3": "edison",
    "cscratch": "cori",
    "bb-shared": "cori",
    "bb-private": "cori",
}

class MountToFsName(object):
    """Converter from mount paths to logical file system names
    """
    def __init__(self):
        """Compile and hash the regular expressions used to identify mount points
        """
        # precompile regular expressions
        self.mount_regex = {}
        for rex_str, fsname in tokio.config.CONFIG.get('mount_to_fsname', {}).items():
            self.mount_regex[re.compile(rex_str)] = fsname

        # hard code this in.  Why do the Mira benchmarks appear to write to NFS?
        self.mount_regex[re.compile('/project')] = "mira-fs1"

    def convert(self, mount):
        """Convert a mount path into a logical file system name

        Args:
            mount (str): A fully qualified path

        Returns:
            str: Either the logical file system name (if identified) or ``mount``
        """
        if mount == '/':
            return mount

        # try to map mount to a logical file system name
        for mount_rex, fsname in self.mount_regex.items():
            match = mount_rex.match(mount)
            if match:
                return fsname

        return mount

def classify_darshanlog(darshan_file):
    """Return a few key defining attributes about a Darshan log

    Returns a dict of the following form:

        {
            "application": "vpicio_uni",
            "file_system": "scratch3",
            "read_or_write": "write",
            "compute_system": "edison",
            "shared_or_fpp": "shared"
        }

    Args:
        darshan_file (str): Path to a Darshan log file
    """
    darshan_log = tokio.connectors.darshan.Darshan(darshan_file)
    darshan_log.darshan_parser_base()

    features = get_biggest_api(darshan_log)
    features.update(get_biggest_fs(darshan_log))

    # convert mount path to logical file system name
    converter = MountToFsName()
    features['biggest_write_fs'] = converter.convert(features['biggest_write_fs'])
    features['biggest_read_fs'] = converter.convert(features['biggest_read_fs'])

    result = {}

    # read or write benchmark?
    if features['biggest_read_api_bytes'] > (10 * features['biggest_write_api_bytes']):
        result['read_or_write'] = 'read'
        result['file_system'] = features['biggest_read_fs']
        most_files = features['biggest_read_api_files']
    elif features['biggest_write_api_bytes'] > (10 * features['biggest_read_api_bytes']):
        result['read_or_write'] = 'write'
        result['file_system'] = features['biggest_write_fs']
        most_files = features['biggest_write_api_files']
    else:
        result['read_or_write'] = 'unknown'
        result['file_system'] = 'unknown'
        most_files = 0

    # explain which file system is attached to which compute system
    result['compute_system'] = FSNAME_TO_SYSTEM.get(result['file_system'], "unknown")

    # fpp or shared-file?
    files_per_proc = float(most_files)  / float(darshan_log['header']['nprocs'])
    if files_per_proc > 0.90:
        result['shared_or_fpp'] = 'fpp'
    elif files_per_proc < 0.10:
        result['shared_or_fpp'] = 'shared'
    else:
        result['shared_or_fpp'] = 'unknown'

    # start_time and date of the benchmark run
    try:
        result['date'] = datetime.datetime.fromtimestamp(
            darshan_log['header']['start_time']).strftime("%Y-%m-%d")
        result['start_time'] = darshan_log['header']['start_time']
    except (IndexError, KeyError):
        warnings.warn("%s: start_time is undecipherable" % result['log_file'])

    # name of the file
    result['log_file'] = os.path.basename(darshan_file)

    # hash of file
    md5 = hashlib.md5()
    with open(darshan_file, 'rb') as openfile:
        md5.update(openfile.read())
    result['md5'] = md5.hexdigest()

    # name of the binary that generated this log
    try:
        result['application'] = os.path.basename(darshan_log['header']['exe'][0])
    except (IndexError, KeyError):
        warnings.warn("%s: exe is undecipherable" % os.path.basename(darshan_file))

    return result

def get_biggest_api(darshan_data):
    """
    Determine the most-used API and file system based on the Darshan log
    """
    if 'counters' not in darshan_data:
        return {}

    biggest_api = {}
    for api_name in darshan_data['counters']:
        biggest_api[api_name] = {
            'write': 0,
            'read': 0,
            'write_files': 0,
            'read_files': 0,
        }
        for file_path, records in darshan_data['counters'][api_name].items():
            if file_path.startswith('_'):
                continue
            for record in records.values():
                bytes_read = record.get('BYTES_READ')
                if bytes_read: # bytes_read is not None and bytes_read > 0:
                    biggest_api[api_name]['read'] += bytes_read
                    biggest_api[api_name]['read_files'] += 1
                bytes_written = record.get('BYTES_WRITTEN')
                if bytes_written: # bytes_written is not None and bytes_read > 0:
                    biggest_api[api_name]['write'] += bytes_written
                    biggest_api[api_name]['write_files'] += 1

    results = {}
    for readwrite in 'read', 'write':
        key = 'biggest_%s_api' % readwrite
        results[key] = max(biggest_api, key=lambda k, rw=readwrite: biggest_api[k][rw])
        results['%s_bytes' % key] = biggest_api[results[key]][readwrite]
        results['%s_files' % key] = biggest_api[results[key]][readwrite + "_files"]

    return results

def get_biggest_fs(darshan_data):
    """
    Determine the most-used file system based on the Darshan log
    """
    if 'counters' not in darshan_data:
        return {}

    if 'biggest_read_api' not in darshan_data or 'biggest_write_api' not in darshan_data:
        biggest_api = get_biggest_api(darshan_data)
        biggest_read_api = biggest_api['biggest_read_api']
        biggest_write_api = biggest_api['biggest_write_api']
    else:
        biggest_read_api = darshan_data['biggest_read_api']
        biggest_write_api = darshan_data['biggest_write_api']

    biggest_fs = {}
    mounts = list(darshan_data['mounts'].keys())
    for api_name in biggest_read_api, biggest_write_api:
        for file_path in darshan_data['counters'][api_name]:
            if file_path in ('_perf', '_total'): # only consider file records
                continue
            for record in darshan_data['counters'][api_name][file_path].values():
                key = _identify_fs_from_path(file_path, mounts)
                if key is None:
                    key = '_unknown' ### for stuff like STDIO
                if key not in biggest_fs:
                    biggest_fs[key] = {'write': 0, 'read': 0}
                bytes_read = record.get('BYTES_READ')
                if bytes_read is not None:
                    biggest_fs[key]['read'] += bytes_read
                bytes_written = record.get('BYTES_WRITTEN')
                if bytes_written is not None:
                    biggest_fs[key]['write'] += bytes_written

    results = {}
    for readwrite in 'read', 'write':
        key = 'biggest_%s_fs' % readwrite
        results[key] = max(biggest_fs, key=lambda k, rw=readwrite: biggest_fs[k][rw])
        results['%s_bytes' % key] = biggest_fs[results[key]][readwrite]

    return results

def _identify_fs_from_path(path, mounts):
    """
    Scan a list of mount points and try to identify the one that matches the
    given path

    """
    max_match = 0
    matching_mount = None
    for mount in mounts:
        if path.startswith(mount) and len(mount) > max_match:
            max_match = len(mount)
            matching_mount = mount
    return matching_mount

def main(argv=None):
    """
    CLI wrapper around the Darshan connector's I/O methods
    """
    parser = argparse.ArgumentParser(description='parse a darshan log')
    parser.add_argument('darshanlogs', nargs="+", type=str,
                        help='Darshan logs to parse')
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output file name")
    parser.add_argument("-j", "--json", action="store_true",
                        help="Output as json (default: False)")
    parser.add_argument("-t", "--threads", type=int, default=1,
                        help="Number of I/O threads (default: 1)")
    args = parser.parse_args(argv)

    # If passed literal globs, expand them here.  Sometimes required to work
    # around ARG_MAX.
    darshan_logs = []
    for darshan_log in args.darshan_logs:
        if '*' in darshan_log:
            darshan_logs += glob.glob(darshan_log)
        else:
            darshan_logs.append(darshan_log)

    if args.threads == 1:
        # Serial implementation--don't bother with multiprocessing at all
        results = []
        for darshan_log in darshan_logs:
            result = classify_darshanlog(darshan_log)
            results.append(result)
    else:
        # Parallel implementation
        pool = multiprocessing.Pool(args.threads)
        results = pool.map(classify_darshanlog, darshan_logs)

    column_order = [
        # "log_file" is used as index; not a valid column
        "date",
        "compute_system",
        "file_system",
        "application",
        "shared_or_fpp",
        "read_or_write",
        "md5",
    ]
    # Serialize the object
    if args.json:
        if not args.output:
            print(json.dumps(results, indent=4, sort_keys=True))
        else:
            print("Writing output to %s" % args.output)
            json.dump(results, open(args.output, 'w'))
    else:
        dataframe = pandas.DataFrame.from_dict(results) \
                                    .set_index('log_file') \
                                    .sort_values('start_time')
        if not args.output:
            print(dataframe[column_order].to_csv())
        else:
            print("Writing output to %s" % args.output)
            dataframe.to_csv(args.output)

if __name__ == '__main__':
    main()
