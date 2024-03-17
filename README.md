# `build-your-own-redis`: Building Redis in Python

This repository houses a Python implementation of Redis built from scratch, including the core Redis functionality, RESP parser, RESP serializer, a rudimentary RDB parser and support for replication across redis instances.

Key Features:

  - Core Redis Functionality: Store, retrieve, and manage data using key-value pairs, similar to the real Redis.
  - RESP Parser and Serializer: Interact with your Redis clone using clients that speak the Redis Serialization Protocol (RESP).
  - Rudimentary RDB Parser: Load data from Redis RDB files, a common format for Redis backups.
  - Support for PSYNC, FULLRESYNC & WAIT command.
  - Command propagation from Master to Replica, Processing of propagated commands by Replica.
  - Multi replica support. 
