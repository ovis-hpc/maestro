#! /usr/bin/env python3

class NoetcdClient(object):
    """Local-use-only, go-free replacement for etcd3 as used in maestro_ctrl."""
    def __init__(self):
        self.noetc = True
        self.d = dict()
    def put(self, path, val):
        self.d[path] = val
    def delete_prefix(self, p):
        kill = list(filter(lambda x: x.startswith(p), self.d.keys()))
        for k in kill:
            self.d.pop(k)
    def print_all(self):
        for i in self.d.items():
            print(i)
    def test(self):
        self.put("bob", "1")
        self.put("/bar", "1")
        self.put("/foo", "1")
        print(self.d)
        self.delete_prefix("/")
        print(self.d)

import argparse
class MaestroBooleanOptionalAction(argparse.Action):
    """backported from python 3.9 argparse"""
    def __init__(self,
                 option_strings,
                 dest,
                 default=None,
                 type=None,
                 choices=None,
                 required=False,
                 help=None,
                 metavar=None):

        _option_strings = []
        for option_string in option_strings:
            _option_strings.append(option_string)

            if option_string.startswith('--'):
                option_string = '--no-' + option_string[2:]
                _option_strings.append(option_string)

        if help is not None and default is not None:
            help += " (default: %(default)s)"

        super().__init__(
            option_strings=_option_strings,
            dest=dest,
            nargs=0,
            default=default,
            type=type,
            choices=choices,
            required=required,
            help=help,
            metavar=metavar)

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string in self.option_strings:
            setattr(namespace, self.dest, not option_string.startswith('--no-'))

    def format_usage(self):
        return ' | '.join(self.option_strings)

if __name__ == "__main__":
    n = NoetcdClient()
    n.test()
