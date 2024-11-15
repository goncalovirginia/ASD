package protocols.abd.messages;

import org.apache.commons.lang3.tuple.Pair;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class WriteMessage extends ProtoMessage {

    public final static short MSG_ID = 503;

    private final int opSeq;
    private String key;
    private final Pair<Integer, Integer> tag;
    private byte[] value;

    public WriteMessage(int opSeq, String key, Pair<Integer, Integer> tag, byte[] val) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
        this.tag = tag;
        this.value = val;
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
        return "WriteMessage{" +
                "opSeq=" + opSeq +
                ", tag=" + tag +
                '}';
    }

    public static ISerializer<WriteMessage> serializer = new ISerializer<WriteMessage>() {
        @Override
        public void serialize(WriteMessage msg, ByteBuf out) {
            out.writeInt(msg.opSeq);

            byte[] keyBytes = msg.key.getBytes();  
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes); 

            out.writeInt(msg.tag.getLeft()); 
            out.writeInt(msg.tag.getRight()); 

            out.writeInt(msg.value.length);
            out.writeBytes(msg.value);
        }

        @Override
        public WriteMessage deserialize(ByteBuf in) {
            int instance = in.readInt();

            byte[] keyBytes = new byte[in.readInt()];
            in.readBytes(keyBytes);
            String key = new String(keyBytes); 

            int left = in.readInt();
            int right = in.readInt();
            Pair<Integer, Integer> ntag = Pair.of(left, right);

            byte[] dt = new byte[in.readInt()];
            in.readBytes(dt);

            return new WriteMessage(instance, key, ntag, dt);
        }
    };

}
