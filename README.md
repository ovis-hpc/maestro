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

There are two principle commands, maestro and maestro_ctrl. maestro will run
the load balancing daemon, as well as start/configure ldmsd's. maestro_ctrl
parses a yaml ldms cluster configuration file, and loads it into a etcd
key/value store. Both commands are demonstrated below:

    maestro --cluster config/etcd.yaml --prefix orion

    maestro_ctrl --cluster config/etcd.yaml --ldms_config config/orion.yaml --prefix orion

Sampler interval's and offsets can be configured during runtime, by updating the ldmsd yaml
configuration file, and running the maestro_ctrl command to update the etcd cluster with the
new configuration. Maestro will detect updates to sampler intervals and offsets, and make
the proper configuration updates to the running samplers.

## Balance Methods

Maestro can be run to distribute ldms sampler data by producer, or by metric set amongst its L1 aggregators. When balancing
by producer, Maestro will attempt to balance producers (samplers) as evenly as possible across available aggregators.
When balancing by metric set, Maestro will balance metric sets by "set weight" across first level aggregators.
Set weight is calculated using the metric set size along with the update frequency of the set. Higher level aggregators
(e.g. L2 and above) will still be balanced by producer, as additional set balancing at higher levels would be both
redundant and more resource intensive.

The default mode for Maestro balancing is by producer, no additional arguments are required.

    maestro --cluster config/etcd.yaml --prefix orion

To run Maestro to balance producers by metric set, run maestro with the argument "--rebalance sets"

    maestro --cluster config/etcd.yaml --prefix orion --rebalance sets

## Multi-Instance Maesro

Maestro can be run using RAFT protocol to split configuration and monitoring responsibilities across a
cluster of Maestro instances. If a quorum of expected Maestro instances is not detected
in the event of multi-node failue, Maestro will wait until it has quorum before continuing
to monitor/configure ldmsds. Any configured ldmsd's will be left in their current state.

In this mode, balancing and configuration responsibilities are split amongst maestro instances.

In order to run maestro in RAFT mode, configure the etcd.yaml file with the maestro instance's
host names and port numbers you'd like to use using the "maestro_members" keyword. An example is
listed below in the ETCD Cluster Configuration section.

Currently Maestro does not support multiple instances on a single host.

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

```raft yaml
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

maestro_members:
  - host: 10.128.0.7
    port: 4411
  - host: 10.128.0.8
    port: 4411
  - host: 10.128.0.9
    port: 4411
```

###LDMS Cluster Configuration
The primary configuration groups are:
daemons
	- defines LDMS daemons, their hosts, ports, and endpoints
aggregators
	- defines aggregator configuration ldmsd's
samplers
	- defines sampler configuration for ldmsd's
stores
	- defines the various stores for aggregators
plugins
	- dictionary of plugins to be loaded by both stores and samplers

Please see the ldmsd_yaml_parser man page for more detailed documentation.

###Example LDMS Configuration
```yaml
daemons:
  - names : &samplers "sampler-[1-8]"
    hosts : &sampler-hosts "node-[1-8]"
    endpoints :
      - names : &sampler-endpoints "node-[1-8]-[10002-10003]"
        ports : &sampler-ports "[10002-10003]"
        maestro_comm : True
        xprt  : sock
        auth  :
           name : ovis1
           plugin : ovis
           conf : /opt/ovis/etc/secret-root.conf

  - names : &l1-agg "l1-aggs-[1-3]"
    hosts : &l1-agg-hosts "node-[2-4]"
    endpoints :
      - names : &l1-agg-endpoints "node-[2-4]-10401"
        ports : &l1-agg-ports 10401
        maestro_comm : True
        xprt  : sock
        auth  :
          name : munge1
          plugin : munge

       # If maestro_comm : false maestro will not create a transport to the LDMSD at this endpoint but
       # the LDMSD will be configured to listen on endpoints described below
       - names : "l1-listener-[1-3]"
         hosts : "node-[1-3]"
         ports : 10404
         maestro_comm : False
         xprt : sock
          auth :
              name : munge1
              plugin : munge

  - names : &l2-agg "l2-agg"
    hosts : &l2-agg-hosts "node-02"
    endpoints :
      - names : &l2-agg-endpoint "node-02-10104"
        ports : &l2-agg-ports "10104"
        maestro_comm : True
        xprt  : sock
        auth  :
          name : munge1
          plugin : munge

# Aggregator configuration
aggregators:
  - daemons   : *l1-agg
    peers     :
      - daemons   : *samplers
        endpoints : *sampler-endpoints
        reconnect : 20s
        type      : active
        updaters  : # The following are possible usages of each updater mode
          - mode     : auto
            interval : "1.0s"
            offset   : "0ms"
            sets :
              - regex : .*
                field : inst

    # Example prdcr_subscribe configuration
    #   subscribe:
    #     - stream: kokkos-perf-data # Stream name
    #       regex: .* # Regular expression that matches producer names
    #       rx_rate   : -1 # Optional. Receive rate (bytes/sec) limit for this connection. Default -1 is unlimited.
    #

  - daemons : *l2-agg
    peers :
      - daemons : *l1-agg
        endpoints : *l1-agg-endpoints
        reconnect : 20s
        type: active
        updaters:
          - mode : auto
            interval : "1.0s"
            offset   : "0ms"
            #sets     : # sets dictionary is not required, defaults to all update from all producer metric sets
            #  - regex : meminfo
            #    field : schema

samplers:
  - daemons : *samplers
    plugins : [ meminfo1, vmstat1 ]

plugins:
  meminfo1 :
    name   : meminfo # Variables can be specific to plugin
    interval : 1.0s # Used when starting the sampler plugin
    offset   : "0s"
    config : # If instance/producer are not defined, producer defaults to ${HOSTNAME} and instance defaults to <producer name>/<plugin name>
      - schema : meminfo
        perm : "0777"

  vmstat1 :
    name   : vmstat
    interval : "8.0s"
    offset   : "1ms"
    config :
      - schema : vmstat
        perm  : "0777"

  procstat1 :
    name     : procstat
    interval : "4.0s"
    offset   : 2ms
    config :
      - schema : procstat
        perm : "0777"

  store_sos1 :
    name : store_sos
    config : [{ path : /home/DATA}]

  csv1 :
    name : store_csv
    config :
        - path        : /DATA/csv/
          altheader   : 0
          typeheader  : 1
          create_uid  : 3031
          create_gid  : 3031

# Storage policy configuration
stores :
  sos-meminfo :
    daemons   : *l2-agg
    container : ldms_data
    schema    : meminfo
    flush     : 10s
    plugin : store_sos1

  sos-vmstat :
    daemons   : *l2-agg
    container : ldms_data
    schema    : vmstat
    flush     : 10s
    plugin : store_sos1

  csv-procstat
    daemons   : *l2-agg
    container : ldms_data
    schema    : procstat
    flush     : 10s
    plugin : csv1
```
