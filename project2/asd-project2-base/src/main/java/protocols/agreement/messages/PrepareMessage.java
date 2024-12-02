package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage extends ProtoMessage {

    public final static short MSG_ID = 106;

    private final int sequenceNumber;
    private final int instance;
    private final boolean ok;

    public PrepareMessage(int sequenceNumber, int instance, boolean ok) {
        super(MSG_ID);
        this.sequenceNumber = sequenceNumber;
        this.instance = instance;
        this.ok = ok;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
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
                "sequenceNumber=" + sequenceNumber +
                ", instance=" + instance +
                ", ok=" + ok +
                '}';
    }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.sequenceNumber);
            out.writeInt(msg.instance);
            out.writeBoolean(msg.ok);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            int sequenceNumber = in.readInt();
            int instance = in.readInt();
            boolean ok = in.readBoolean();
            return new PrepareMessage(sequenceNumber, instance, ok);
        }
    };

}
