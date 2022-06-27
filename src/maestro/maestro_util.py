import collections
from . import hostlist

AUTH_ATTRS = [
    'auth',
    'conf'
]

CORE_ATTRS = [
    'daemons',
    'aggregators',
    'samplers',
    'stores'
]

DEFAULT_ATTR_VAL = {
    'maestro_comm' : False,
    'xprt'         : 'sock',
    'interval'     : 1000000,
    'auth'         : 'none',
    'mode'         : 'static'
}

INT_ATTRS = [
    'interval',
    'offset',
    'reconnect'
]

def cvt_intrvl_str_to_us(interval_s):
    """Converts a time interval string to microseconds

    A time-interval string is an integer or float follows by a
    unit-string. A unit-string is any of the following:

    's'  - seconds
    'ms' - milliseconds
    'us' - microseconds
    'm'  - minutes

    Unit strings are not case-sensitive.

    Examples:
    '1.5s' - 1.5 seconds
    '1.5S' - 1.5 seconds
    '2s'   - 2 seconds
    """
    error_str = f"{interval_s} is not a valid time-interval string\n"\
                f"'Only a single unit-string is allowed. e.g. '50s40us' is not a valid entry."\
                f"Examples of acceptable format:\n"\
                f"'1.5s' - 1.5 seconds\n"\
                f"'1.5S' - 1.5 seconds\n"\
                f"'2us'  - 2 microseconds\n"\
                f"'3m'   - 3 minutes\n"\
                f"\n"
    if type(interval_s) == int:
        return interval_s
    if type(interval_s) != str:
        raise ValueError(f"{error_str}")
    interval_s = interval_s.lower()
    if 'us' in interval_s:
        factor = 1
        if interval_s.split('us')[1] != '':
            raise ValueError(f"{error_str}")
        ival_s = interval_s.split('us')[0]
    elif 'ms' in interval_s:
        factor = 1000
        if interval_s.split('ms')[1] != '':
            raise ValueError(f"{error_str}")
        ival_s = interval_s.split('ms')[0]
    elif 's' in interval_s:
        factor = 1000000
        if interval_s.split('s')[1] != '':
            raise ValueError(f"{error_str}")
        ival_s = interval_s.split('s')[0]
    elif 'm' in interval_s:
        factor = 60000000
        if interval_s.split('m')[1] != '':
            raise ValueError(f"{error_str}")
        ival_s = interval_s.split('m')[0]
    else:
        ival_s = interval_s
        factor = 1
    try:
        mult = float(ival_s)
    except:
        raise ValueError(f"{interval_s} is not a valid time-interval string")
    return int(mult * factor)

def check_offset(interval_us, offset_us=None):
    if offset_us:
        interval_us = int(interval_us)
        offset_us = int(offset_us)
        if offset_us/interval_us > .5:
            offset_us = interval_us/2
    else:
        offset_us = 0
    return offset_us

def check_opt(attr, spec):
    # Check for optional argument and return None if not present
    if attr in AUTH_ATTRS:
        if attr == 'auth':
            attr = 'name'
        if 'auth' in spec:
            spec = spec['auth']
    if attr in spec:
        if attr in INT_ATTRS:
            return cvt_intrvl_str_to_us(spec[attr])
        return spec[attr]
    else:
        if attr in DEFAULT_ATTR_VAL:
            return DEFAULT_ATTR_VAL[attr]
        else:
            return None

def check_required(attr_list, container, container_name):
    """Verify that each name in attr_list is in the container"""
    for name in attr_list:
        if name not in container:
            raise ValueError("The '{0}' attribute is required in a {1}".
                             format(name, container_name))

def expand_names(name_spec):
    if type(name_spec) != str and isinstance(name_spec, collections.Sequence):
        names = []
        for name in name_spec:
            names += hostlist.expand_hostlist(name)
    else:
        names = hostlist.expand_hostlist(name_spec)
    return names

def parse_to_cfg_str(cfg_obj):
    cfg_str = ''
    for key in cfg_obj:
        if key not in INT_ATTRS:
            if len(cfg_str) > 8:
                cfg_str += ' '
            cfg_str += key + '=' + str(cfg_obj[key])
    return cfg_str

def parse_yaml_bool(bool_):
    if bool_ is True or bool_ == 'true' or bool_ == 'True':
        return True
    else:
        return False
