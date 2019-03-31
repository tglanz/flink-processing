package com.tglanz.sources.udp;

import java.net.DatagramSocket;
import java.net.DatagramPacket;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class UdpSource extends RichSourceFunction<Entry> {

    private final int port;
    private final int bufferSize;

    private volatile boolean shouldRun;

    public UdpSource(int port, int bufferSize) {
        this.shouldRun = true;
        this.port = port;
        this.bufferSize = bufferSize;
    }

    @Override
    public void run(SourceContext<Entry> ctx) throws Exception {
        DatagramSocket socket = new DatagramSocket(this.port);

        try {
            byte[] buffer = new byte[this.bufferSize];

            while (this.shouldRun){
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String dataString = new String(packet.getData(), 0, packet.getLength());
                ctx.collect(new Entry(dataString));
            }
        } finally {
            socket.close();
        }
    }

    @Override
    public void cancel() {
        this.shouldRun = false;
    }
}