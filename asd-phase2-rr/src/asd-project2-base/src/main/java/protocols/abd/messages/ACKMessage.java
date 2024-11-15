package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ACKMessage extends ProtoMessage {

    public final static short MSG_ID = 504;

    private final int opSeq;
    private String key;

    public ACKMessage(int opSeq, String key) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
    }

    public int getOpId() {
        return opSeq;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "ACKMessage{" +
                "opSeq=" + opSeq +
                ", key=" + key +
                '}';
    }

    public static ISerializer<ACKMessage> serializer = new ISerializer<ACKMessage>() {
        @Override
        public void serialize(ACKMessage msg, ByteBuf out) {
            out.writeInt(msg.opSeq);

            byte[] keyBytes = msg.key.getBytes();  
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes); 
        }

        @Override
        public ACKMessage deserialize(ByteBuf in) {
            int instance = in.readInt();

            byte[] keyBytes = new byte[in.readInt()];
            in.readBytes(keyBytes);
            String key = new String(keyBytes); 

            return new ACKMessage(instance, key);
        }
    };

}
