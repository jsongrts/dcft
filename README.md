# dcft
This is a trivial implementation, in Java, of DCFT pattern proposed by the reserach paper

Stutsman, Ryan, Collin Lee, and John Ousterhout. "Experience with Rules-Based Programming for Distributed, Concurrent, Fault-Tolerant Code." 2015 USENIX Annual Technical Conference (USENIX ATC 15). USENIX Association, 2015.

It implements the busy-polling version (for RPC completion) and it's easy to be converted into a blocking version.

# Introduction
This program implements a trivial membership service. A coordinator (which isn't dynamically elected among nodes. Instead, which is specified
statically by techops) tracks all active clients. If there are any change to the set of active clients (e.g. new clients join, some clients go down) 
the new member list will be sent to all active clients.

# Client-coordinator Protocol
Clients and the coordinator use Google protocol buffer to exchange messages. There are 3 messages:
- heartbeat - the client sends a heartbeat message to the coordinator every second. If the coordinator doesn't get heartbeats 
from a client for 5 seconds, it will mark the client as down.
- setMemberList - whenever the member list is changed, the coordinator will push the latest member list to all connected clients
- setMemberListResponse - the client should respond with a setMemberListResponse message upon receiving a setMemberList message.

The client is responsible for establishing a TCP connection to the coordinator. 

# Dcft Implementation
The program implements a simple asynchrnous RPC system based on Netty and Java FutureTask.

The system has a single DCFT task.

# Build and Run
To build
  - gradle build && gradle fatJar
  
To run the coordinator
  - java -cp build/libs/dcft-1.0.0.jar com.logicmonitor.research.dcft.server2.Main
  
To run the client
  - java -cp build/libs/dcft-1.0.0.jar com.logicmonitor.research.dcft.client.Main $clientId

# Tags
distributed, concurrency, fault tolerance, jdk executor, netty, google protocol buffer, dcft
