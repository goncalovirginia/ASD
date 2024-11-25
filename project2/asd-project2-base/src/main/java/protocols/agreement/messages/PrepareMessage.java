package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage extends ProtoMessage {

    public final static short MSG_ID = 106;

    private final int seqNumber;
    private final int instance;
    private final boolean ok;

    public PrepareMessage(int seqNumber, int instance, boolean ok) {
        super(MSG_ID);
        this.seqNumber = seqNumber;
        this.instance = instance;
        this.ok = ok;
    }

    public int getSeqNumber() {
        return seqNumber;
    }

    public int getInstance() {
        return instance;
    }

    public boolean isOK() {
        return ok;
    }

    @Override
    public String toString() {
        return "PrepareMessage{" +
                "seqNumber=" + seqNumber +
                ", instance=" + instance +
                '}';
    }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.seqNumber);
            out.writeInt(msg.instance);
            out.writeBoolean(msg.ok);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            int sn = in.readInt();
            int instance = in.readInt();
            boolean ok = in.readBoolean();
            return new PrepareMessage(sn, instance, ok);
        }
    };

}
