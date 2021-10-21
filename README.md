Usage:
## compile code 
```
mkdir build && cd build && cmake .. && make 

```

## run 
```
# shard experiment controller
./shard_ctl <ctrl-port> <num-clients> <repeat-exp-n-times>

# server
./shard_server <server-port>


# shard client
./shard_cli <ctrl-ip> <ctrl-port> <n-servers> <server-1-ip> <server-1-port> ...

```

