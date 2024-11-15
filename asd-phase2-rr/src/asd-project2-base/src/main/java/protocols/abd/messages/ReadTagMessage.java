package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ReadTagMessage extends ProtoMessage {

    public final static short MSG_ID = 501;

    private final int opSeq;
    private final String key;

    public ReadTagMessage(int opSeq, String key) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "ReadTagMessage{" +
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
        }

        @Override
        public ReadTagMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            int keyLength = in.readInt();
            byte[] keyBytes = new byte[keyLength];
            in.readBytes(keyBytes);
            
            String key = new String(keyBytes); 

            return new ReadTagMessage(instance, key);
        }
    };

}
