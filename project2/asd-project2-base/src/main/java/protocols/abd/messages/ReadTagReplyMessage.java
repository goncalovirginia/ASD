package protocols.abd.messages;

import org.apache.commons.lang3.tuple.Pair;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ReadTagReplyMessage extends ProtoMessage {

    public final static short MSG_ID = 502;

    private final int opSeq;
    private final Pair<Integer, Integer> tag;
    private final String key;

    public ReadTagReplyMessage(int opSeq, Pair<Integer, Integer> tag, String key) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.tag = tag;
        this.key = key;
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

    @Override
    public String toString() {
        return "ReadTagReplyMessage{" +
                "opSeq=" + opSeq +
                ", key=" + key +
                ", tag=" + tag +
                '}';
    }

    public static ISerializer<ReadTagReplyMessage> serializer = new ISerializer<ReadTagReplyMessage>() {
        @Override
        public void serialize(ReadTagReplyMessage msg, ByteBuf out) {
            out.writeInt(msg.opSeq);
            out.writeInt(msg.tag.getLeft()); 
            out.writeInt(msg.tag.getRight());
            
            byte[] keyBytes = msg.key.getBytes();  
            out.writeInt(keyBytes.length);
            out.writeBytes(keyBytes); 
        }

        @Override
        public ReadTagReplyMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            int left = in.readInt();
            int right = in.readInt();
            Pair<Integer, Integer> ntag = Pair.of(left, right);

            byte[] keyBytes = new byte[in.readInt()];
            in.readBytes(keyBytes);
            String key = new String(keyBytes); 

            return new ReadTagReplyMessage(instance, ntag, key);
        }
    };

}
