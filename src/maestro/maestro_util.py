def fmt_cmd_args(comm, cmd, spec):
    cfg_args = {}
    cmd_attr_list = comm.get_cmd_attr_list(cmd)
    for key in spec:
        if key in cmd_attr_list['req'] or key in cmd_attr_list['opt']:
            if key == 'plugin':
                cfg_args[key] = spec[key]
                continue
            cfg_args[key] = spec[key]
    if not all(key in spec for key in cmd_attr_list['req']):
        print(f'The attribute(s) {set(cmd_attr_list["req"]) - spec.keys()} are required by {cmd}')
        raise ValueError()
    return cfg_args

