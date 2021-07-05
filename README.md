# Maestro
LDMS Monitoring Cluster Load Balancing Service

Maestro is a Python3 implementation of a service that load balances
a cluster configuration across a number of configured Aggregators.

Aggregators are configured in groups, and groups are organized into a
hierarchy. The lowest level of the hierarchy (level 1) communicate
with the sampler daemons; 2nd level aggregators monitor 1st level
aggregators, 3rd level aggregators monitor 2nd level aggregators
and so on.

There are multiple goals of the __maestro__ daemon:
* Simplify the monitoring configuration of a large cluster
* Create a more resilient and responsive monitoring infrastructure
* Monitor the health and performance of aggregators in the cluster
* Manage aggregator failure by rebalancing the distributed
configuration across the remaining aggregators in a group

In this current release, Maestro does not start and start __ldmsd__
daemons, however, this feature is planned for the future.

## Dependencies
etcd3 can be downlaoded via pip3, or from source at:
https://github.com/etcd-io/etcd

## Configuration Generation
A ldms cluster's configuration can be generated with the maestro_ctrl
command. The current implementation of the generator will generate 
ldmsd configuration files for each group in the ldmsd cluster config.
The generator will automatically "balance" samplers across aggregators,
if sampler's are loaded by multiple aggregators, effectively creating
a unique configuration file for each aggregator in a group.
 The command to generate config files is listed below:
	maestro_ctrl --cluster config/etcd.yaml --ldms_config config/ldms-config.yaml --prefix <cluster_name> --generate-config-path <config_directory_path>

## Configuration Management

A cluster's configuration is mangaged in a distributed RAFT based
key-value store called __etcd__. The __etcd__ service is queried using
the python _etcd3_ client interface.

There are two configuration files consumed by __maestro__:
1. A file that defines the _etcd_ cluster configuration
2. A file that defines the LDMS Cluster Configuration

There are two principle commands, maestro and maestro_ctrl. maestro will run the load balancing daemon, as well as start/configure the ldmsd's. maestro_ctrl parses a yaml ldms cluster configuration file, and loads it into the etcd cluster configuration. Both commands are demonstrated below:

    maestro --cluster config/etcd.yaml --prefix orion

    maestro_ctrl --cluster config/etcd.yaml --ldms_config config/orion.yaml --prefix orion

Sampler interval's and offsets can be configured during runtime, by updating the ldmsd yaml
configuration file, and running the maestro_ctrl command to update the etcd cluster with the
new configuration. Maestro will detect updates to sampler intervals and offsets, and make 
the proper configuration updates to the running samplers.

### ETCD Cluster Configuration

Maestro consumes configuration files in YAML format.

Here's an example of an _etcd_ cluster configuration:

```yaml
cluster: voltrino
members:
  - host: 10.128.0.7
    port: 2379
members:
  - host: 10.128.0.8
    port: 2379
members:
  - host: 10.128.0.9
    port: 2379
```

And here is an example of a LDMS Cluster Configuration File:

```yaml
endpoints:
  - names : &sampler-endpoints "nid[00012-00200]"
    group : samplers
    hosts : &sampler-hosts "nid[00012-00200]"
    ports : "10001"
    xprt : sock
    auth :
      name  : munge
      config  :
          domain : samplers

  - names : &l1-agg-endpoints "agg-[11-14]"
    group : &l1 "l1-agg"
    hosts : &l1-agg-hosts "nid00002"
    ports : "[30011-30014]"
    xprt : sock
    auth :
      name  : munge
      config  :
        domain : aggregators

  - names : &l2-agg-endpoints "agg-[21,22]"
    group : &l2 "l2-agg"
    hosts : &l2-agg-hosts "nid00003"
    ports : "[30021,30022]"
    xprt : sock
    auth :
      name  : munge
      config  :
        domain : aggregators

  - names : &l3-agg-endpoints "agg-31"
    group : &l3 "l3-agg"
    hosts : &l3-agg-hosts "nid00004"
    ports : "30031"
    xprt : sock
    auth :
      name  : munge
      config  :
        domain : users

groups:
  - endpoints : *sampler-endpoints
    name : samplers
    interfaces :
      - *sampler-hosts

  - endpoints : *l1-agg-endpoints
    name : *l1
    interfaces :
      - *sampler-hosts
      - *l1-agg-endpoints
      - *l2-agg-endpoints

  - endpoints : *l2-agg-endpoints
    name : *l2
    interfaces :
      - *l1-agg-endpoints
      - *l2-agg-endpoints

aggregators:
  - names     : *l1-agg-endpoints
    endpoints : *l1-agg-endpoints
    group     : *l1

  - names     : *l2-agg-endpoints
    endpoints : *l2-agg-endpoints
    group     : *l2

  - names     : *l3-agg-endpoints
    endpoints : *l3-agg-hosts*
    group     : *l3

samplers:
  - names       : *sampler-endpoints
    group : samplers
    config :
      - name        : meminfo # Variables can be specific to plugin
        interval    : "1.0s:0ms" # Interval:offset format. Used when starting the plugin
        perm        : "0777"

      - name        : vmstat
        interval    : "1.0s:0ms" # Interval:offset format. Used when starting the plugin
        perm        : "0777"

producers:
# This informs the L1 load balance group what is being distributed across
# the L1 aggregator nodes
  - names     : *sampler-endpoints
    endpoints : *sampler-endpoints
    group     : *l1
    reconnect : 20s
    type      : active
    updaters  :
      - l1-all

# This informs the L2 load balance group what is being distributed across
# the L2 aggregator nodes
  - names      : *l1-agg-endpoints
    endpoints  : *l1-agg-endpoints
    group      : *l2
    reconnect  : 20s
    type       : active
    updaters   :
      - l2-all

# This informs the L3 load balance group what is being distributed across
# the L3 aggregator node
  - names      : *l2-agg-endpoints
    endpoints  : *l2-agg-endpoints
    group      : *l3
    reconnect  : 20s
    type       : active
    updaters  :
      - l3-all


updaters:
- name  : all           # must be unique within group
  group : *l1
  interval : "1.0s:0ms"
  sets :
    - regex : .*        # regular expression matching set name or schema
      field : inst      # 'instance' or 'schema'
  producers :
    - regex : .*        # regular expression matching producer name
                        # this is evaluated on the Aggregator, not
                        # at configuration time'
- name  : all
  group : *l2
  interval : "1.0s:250ms"
  sets :
    - regex : .*
      field : inst
  producers :
    - regex : .*

- name  : all
  group : *l3
  interval : "1.0s:500ms"
  sets :
    - regex : .*
      field : inst
  producers :
    - regex : .*

stores :
  - name      : sos-meminfo
    group     : *l3
    container : ldms_data
    schema    : meminfo
    plugin :
      name   : store_sos
      config : { path            : /DATA15/orion,
                 commit_interval : 600
      }

  - name      : sos-vmstat
    group     : *l3
    container : ldms_data
    schema    : vmstat
    plugin :
      name   : store_sos
      config : { path : /DATA15/orion }

  - name      : sos-procstat
    group     : *l3
    container : ldms_data
    schema    : procstat
    plugin :
      name   : store_sos
      config : { path : /DATA15/orion }

  - name : csv
    group     : *l3
    container : ldms_data
    schema    : meminfo
    plugin :
      name : store_csv
      config :
        path        : /DATA15/orion/csv/orion
        altheader   : 0
        typeheader  : 1
        create_uid  : 3031
        create_gid  : 3031
```

