package com.logicmonitor.research.dcft.client;

/**
 * Created by jsong on 7/21/15.
 *
 * The client establishes a TCP connection to the coordinator when starting up. Over the
 * connection, it sends a heartbeat message periodically and receives the membership list.
 *
 * If the connection is disconnected, it will try to re-connection.
 *
 * java -cp dcft.jar com.logicmonitor.research.dcft.client.Main $cliId [$rpcDelayInSec]
 */
public class Main {
    public static void main(String[] args) throws Exception {
        int rpcDelayInSecs = args.length >= 2 ? Integer.parseInt(args[1]) : 0;
        DcftClient cli = new DcftClient("127.0.0.1", 8989, Integer.parseInt(args[0]), rpcDelayInSecs);
        cli.run();
    }
}
