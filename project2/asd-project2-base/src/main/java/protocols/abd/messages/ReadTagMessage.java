package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ReadTagMessage extends ProtoMessage {

    public final static short MSG_ID = 501;

    private final int opSeq;
    private final String key;
    private final boolean reading;

    public ReadTagMessage(int opSeq, String key, boolean r) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.reading = r;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public String getKey() {
        return key;
    }

    public boolean isReading() {
        return reading;
    }

    @Override
    public String toString() {
        String start = "";
        if (reading)
            start = "ReadMessage{";
        else
            start = "ReadTagMessage{";

        return  start +
                "opSeq=" + opSeq +
                ", key=" + key +
                '}';
    }

    public static ISerializer<ReadTagMessage> serializer = new ISerializer<ReadTagMessage>() {
        @Override
        public void serialize(ReadTagMessage msg, ByteBuf out) {
            out.writeInt(msg.opSeq);
            byte[] keyBytes = msg.key.getBytes();  
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes); 
            out.writeBoolean(msg.reading);
        }

        @Override
        public ReadTagMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            int keyLength = in.readInt();
            byte[] keyBytes = new byte[keyLength];
            in.readBytes(keyBytes);
            
            String key = new String(keyBytes); 
            boolean reading = in.readBoolean();

            return new ReadTagMessage(instance, key, reading);
        }
    };

}
