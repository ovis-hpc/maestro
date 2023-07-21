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

## Install / Uninstall

```sh
$ pip3 install --upgrade --prefix /opt/ovis .
# This assumed that LDMS is installed with /opt/ovis prefix.
# Use `upgrade` option so that the existing installation will get upgrade
# from the source tree. Otherwise, the existing files/directories were left
# untouched.

# to uninstall
$ pip3 uninstall maestro
```

## Dependencies
etcd3 can be downlaoded via pip3, or from source at:
https://github.com/etcd-io/etcd

## Configuration Generation
A ldms cluster's configuration can be generated with the maestro_ctrl
command. The current implementation of the generator will generate
v4 ldmsd configuration files for each group in the ldmsd cluster config.
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

There are two principle commands, maestro and maestro_ctrl. maestro will run the load balancing daemon, as well as start/configure ldmsd's. maestro_ctrl parses a yaml ldms cluster configuration file, and loads it into a etcd key/value store. Both commands are demonstrated below:

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

###LDMS Cluster Configuration
The primary configuration groups are
daemons
	- defines LDMS daemons, their hosts, ports, and endpoints
aggregators
	- defines aggregator configuration ldmsd's
samplers
	- defines sampler configuration for ldmsd's
stores
	- defines the various stores for aggregators

###Example LDMS Configuration
```yaml
daemons:
  - names : &samplers "sampler-[1-10]"
    hosts : &node-hosts "node-[1-10]"
    endpoints :
      - names : &sampler-endpoints "node-[1-10]-[10002]"
        ports : &sampler-ports "[10002]"
        maestro_comm : True
        xprt  : sock
        auth  :
           name : ovis1 # The authentication domain name
           plugin : ovis # The plugin type
           conf : /opt/ovis/maestro/secret.conf

      - names : &sampler-rdma-endpoints "node-[1-10]-10002/rdma"
        ports : *sampler-ports
        maestro_comm : False
        xprt  : rdma
        auth  :
          name : munge1
          plugin : munge

  - names : &l1-agg "l1-aggs-[11-14]"
    hosts : &l1-agg-hosts "node-[11-14]"
    endpoints :
      - names : &l1-agg-endpoints "node-[11-14]-[10101]"
        ports : &agg-ports "[10101]"
        maestro_comm : True
        xprt  : sock
        auth  :
          name : munge1
          plugin : munge

  - names : &l2-agg "l2-agg"
    hosts : &l2-agg-host "node-15"
    endpoints :
      - names : &l2-agg-endpoints "node-[15]"
        ports : "[10104]"
        maestro_comm : True
        xprt  : sock
        auth  :
          name : munge1
          plugin : munge

aggregators:
  - daemons   : *l1-agg
    peers     :
      - endpoints : *sampler-endpoints
        reconnect : 20s
        type      : active
        updaters  :
          - mode     : pull
            interval : "1.0s"
            offset   : "0ms"
            sets     :
              - regex : .*
                field : inst

  - daemons   : *l2-agg
    peers     :
      - endpoints : *l1-agg-endpoints
        reconnect : 20s
        type      : active
        updaters  :
          - mode : pull
            interval : "1.0s"
            offset   : "0ms"
            sets     :
              - regex : .*
                field : inst

samplers:
  - daemons : *samplers
    plugins :
      - name        : meminfo # Variables can be specific to plugin
        interval    : "1.0s" # Used when starting the sampler plugin
        offset      : "0ms"
        config      : # Config is a list of dictionaries or strings that defines a plugin configuration
                      # A config command will be submitted for each string/dictionary in the list
          - schema       : meminfo
            component_id : ${LDMS_COMPONENT_ID} # uses an environment variable to set the component_id
            producer     : ${HOSTNAME}
            instance     : ${HOSTNAME}/meminfo
            perm         : "0o777"

      - name        : vmstat
        interval    : "1.0s"
        offset      : "0ms"
        config      :
          - "schema=vmstat producer=${HOSTNAME} instance=${HOSTNAME}/vmstat perm=0o777"

      - name        : procstat
        interval    : "1.0s"
        offset      : "0ms"
        config      : [ "schema=vmstat producer=${HOSTNAME} instance=${HOSTNAME}/procstat perm=0o777" ] 

stores:
  - name      : sos-meminfo
    daemons   : *l2-agg
    container : ldms_data
    schema    : meminfo
    flush     : 10s
    plugin : # Store plugin have the same configuration format as sampler plugins e.g. list of strings and or dictionaries
      name   : store_sos
      config : [ { path : /DATA } ]

  - name      : sos-vmstat
    daemons   : *l2-agg
    container : ldms_data
    schema    : vmstat
    flush     : 10s
    plugin :
      name   : store_sos
      config : [ { path : /DATA } ]

  - name      : sos-procstat
    daemons   : *l2-agg
    container : ldms_data
    schema    : procstat
    flush     : 10s
    plugin :
      name   : store_sos
      config : [ { path : /DATA } ]

  - name : csv
    daemons   : *l2-agg
    container : ldms_data
    schema    : meminfo
    plugin :
      name : store_csv
      config :
        - path        : /DATA/csv/
          altheader   : 0
          typeheader  : 1
          create_uid  : 3031
          create_gid  : 3031
```

