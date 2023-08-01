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
    'reconnect',
    'flush'
]

unit_strs = [
    'ms',
    'us',
    'm',
    's',
    'h',
    'd'
]
LDMS_YAML_ERR = 'Error parsing ldms_config yaml file'
LIST_ERR = 'spec must be a list of dictionaries, specified with "-" in the ldms_config yaml file'

def check_intrvl_str(interval_s):
    """Check the format of the interval string is correct

    A time-interval string is an integer or float followed by a
    unit-string. A unit-string is any of the following:

    'us' - microseconds
    'ms' - milliseconds
    's'  - seconds
    'm'  - minutes
    'h'  - hours
    'd'  - days

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
                f"'1h'   - 1 hour\n"\
                f"'1d'   - 1 day\n"\
                f"\n"
    if type(interval_s) == int:
        return interval_s
    if type(interval_s) != str:
        raise ValueError(f"{error_str}")
    interval_s = interval_s.lower()
    unit = next((unit for unit in unit_strs if unit in interval_s), None)
    if unit:
        if interval_s.split(unit)[1] != '':
            raise ValueError(f"{error_str}")
        ival_s = interval_s.split(unit)[0]
    else:
        ival_s = interval_s
    try:
        mult = float(ival_s)
    except Exception as e:
        raise ValueError(f"{interval_s} is not a valid time-interval string")
    return interval_s

def check_opt(attr, spec):
    # Check for optional argument and return None if not present
    if attr in AUTH_ATTRS:
        if attr == 'auth':
            attr = 'name'
        if 'auth' in spec:
            spec = spec['auth']
    if attr in spec:
        if attr in INT_ATTRS:
            return check_intrvl_str(spec[attr])
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

def fmt_cmd_args(comm, cmd, spec):
    cfg_args = {}
    cmd_attr_list = comm.get_cmd_attr_list(cmd)
    for key in spec:
        if key in cmd_attr_list['req'] or key in cmd_attr_list['opt']:
            if key == 'plugin':
                cfg_args[key] = spec[key]['name']
                continue
            cfg_args[key] = spec[key]
    if not all(key in spec for key in cmd_attr_list['req']):
        print(f'The attribute(s) {set(cmd_attr_list["req"]) - spec.keys()} are required by {cmd}')
        raise ValueError()
    return cfg_args

def NUM_STR(obj):
    return str(obj) if type(obj) in [ int, float ] else obj

def expand_names(name_spec):
    if type(name_spec) != str and isinstance(name_spec, collections.abc.Sequence):
        names = []
        for name in name_spec:
            names += hostlist.expand_hostlist(NUM_STR(name))
    else:
        names = hostlist.expand_hostlist(NUM_STR(name_spec))
    return names

def parse_to_cfg_str(cfg_obj):
    cfg_str = ''
    for key in cfg_obj:
        if key not in INT_ATTRS:
            if len(cfg_str) > 1:
                cfg_str += ' '
            cfg_str += key + '=' + str(cfg_obj[key])
    return cfg_str

def parse_yaml_bool(bool_):
    if bool_ is True or bool_ == 'true' or bool_ == 'True':
        return True
    else:
        return False
