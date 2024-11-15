package protocols.abd.messages;

import org.apache.commons.lang3.tuple.Pair;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ReadReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 505;

    private final int opSeq;
    private final Pair<Integer, Integer> tag;
    private final String key;
    private final byte[] value;

    public ReadReplyMessage(int opSeq, Pair<Integer, Integer> tag, String key, byte[] value) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.tag = tag;
        this.key = key;
        this.value = value;
    }

    public int getOpId() {
        return opSeq;
    }

    public String getKey() {
        return key;
    }

    public Pair<Integer, Integer> getTag() {
        return tag;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "ReadReplyMessage{" +
                "opSeq=" + opSeq +
                ", key=" + key +
                ", tag=" + tag +
                '}';
    }

    public static ISerializer<ReadReplyMessage> serializer = new ISerializer<ReadReplyMessage>() {
        @Override
        public void serialize(ReadReplyMessage msg, ByteBuf out) {
            out.writeInt(msg.opSeq);
            out.writeInt(msg.tag.getLeft()); 
            out.writeInt(msg.tag.getRight());
            
            byte[] keyBytes = msg.key.getBytes();  
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes); 

            out.writeInt(msg.value.length);
            out.writeBytes(msg.value);
        }

        @Override
        public ReadReplyMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            int left = in.readInt();
            int right = in.readInt();
            Pair<Integer, Integer> ntag = Pair.of(left, right);

            byte[] keyBytes = new byte[in.readInt()];
            in.readBytes(keyBytes);
            String key = new String(keyBytes); 

            byte[] dt = new byte[in.readInt()];
            in.readBytes(dt);

            return new ReadReplyMessage(instance, ntag, key, dt);
        }
    };

}
