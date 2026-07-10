# RTC-BENCH

This tool can be used to benchmark webrtc applications deployed in kubernetes clusters with [STUNner](https://github.com/l7mp/stunner).

# Preparation

## Adding the remote clusters.

The tool works from the local kubeconfig file. To be able to reach remote clusters, we have to add the necessary information:
- Cluster
- User
- Context

You can copy these information from the remote machines config. Make sure to modify the server address in the cluster entry from the loopback address.