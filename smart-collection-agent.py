#!/usr/bin/env python

import argparse
import ConfigParser
import datadotworld as dw
import os
import os.path
import subprocess
import sys
import time

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--config', default='', required=False,
                        help=('Configuration file to read options from. By '
                              'default, it looks for '
                              '/etc/smart-collection-agent.ini and uses '
                              'default options if it does not exist.'))
arg_parser.add_argument('--devices', default='', required=False,
                        help=('Comma-separated list of devices to monitor. By'
                              'default, look at all available hard disks.'))
args = arg_parser.parse_args()
config = {}


class MyIniParser(ConfigParser.ConfigParser):
    def as_dict(self):
        d = dict(self._sections)
        for k in d:
            d[k] = dict(self._defaults, **d[k])
            d[k].pop('__name__', None)
        return d


def get_default_devices():
    return ['/dev/sda']


def get_default_config():
    return {
        'update_interval_secs': 86400,
        'dw_username': 'ivotron',
        'dw_dataset': 's-m-a-r-t',
        'dw_token': '',
        'devices': get_default_devices()
    }


def get_config_from_file():
    config_from_file = {}

    ini_parser = MyIniParser()

    if os.path.isfile('/etc/smart-collection-agent.ini'):
        ini_parser.read('/etc/smart-collection-agent.ini')
        config_from_file = ini_parser.as_dict()

    if args.config != "":
        if not os.path.isfile(args.config):
            raise Exception("Unable to find file {}".format(args.config))
        ini_parser.read(args.config)
        config_from_file = ini_parser.as_dict()

    return config_from_file


def get_config():
    config = get_default_config()
    config.update(get_config_from_file())
    return config


def request_for_stats():
    subprocess.Popen(['smartctl', '-t', 'short'] + config['devices'].split(','))


def get_results():
    return "2013-09-01,W300D3HY,ST4000DM000,4000787030016,0,,171900064,,,,,,,,0,,,,,,1865,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,20,,,,,,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,"


def upload(data):
    if not config['dw_token'] and not os.environ.get('DW_AUTH_TOKEN', None):
        raise Exception("Expecting data.world token.")
    dataset = '{}/{}'.format(config['dw_username'], config['dw_dataset'])
    with dw.open_remote_file(dataset, 'data.csv') as w:
        w.write(data)


def obtain_stats():
    request_for_stats()
    time.sleep(300)
    upload(get_results())


def main():
    global config
    config = get_config()
    while True:
        obtain_stats()
        sys.exit(0)
        time.sleep(config['update_interval_secs'])


if __name__ == "__main__":
    main()
