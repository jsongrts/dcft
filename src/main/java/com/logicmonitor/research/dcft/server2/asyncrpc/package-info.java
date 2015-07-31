/**
 * Created by jsong on 7/29/15.
 *
 * This package implements a trivial asynchronous RPC package.
 *
 * Each client has an unique id. The package maintains a map from the
 * client id to the corresponding Channel.
 *
 * The client should actively establish the channel to the coordinator.
 *
 * Every RPC has a unique transaction id.
 *
 * Every RPC has a timeout of 5 seconds.
 */
package com.logicmonitor.research.dcft.server2.asyncrpc;