package protocols.abd;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.abd.requests.ReadRequest;
import protocols.abd.requests.WriteRequest;
import protocols.abd.messages.ACKMessage;
import protocols.abd.messages.ReadReplyMessage;
import protocols.abd.messages.ReadTagMessage;
import protocols.abd.messages.ReadTagReplyMessage;
import protocols.abd.messages.WriteMessage;
import protocols.abd.renotifications.ReadCompleteNotification;
import protocols.abd.renotifications.UpdateValueNotification;
import protocols.abd.renotifications.WriteCompleteNotification;
import protocols.statemachine.notifications.ChannelReadyNotification;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class ABD extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(ABD.class);

    public static final String PROTOCOL_NAME = "ABD";
    public static final short PROTOCOL_ID = 500;

    private final Host self;     //My own address/port
    private final int channelId; //Id of the created channel

    private List<Host> membership;
    private int nextInstance; //opSeq
    private int processSequence; //processSequence

    //private Map<Integer, List<Pair<Integer, Integer>>> roundAnswers; 
    private List<Pair<Pair<Integer, Integer>, byte[]>> answers;

    private final Map<String, Pair<Integer, Integer>> tags; //HashMap key-(opSeq, processSeq) pair
    private final Map<String, byte[]> values; //HashMap key-values
    private final Map<String, UUID> operations; //HashMap key-operations

    private byte[] pending; //value pending to be written

    public ABD(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        nextInstance = 0;
        answers = new LinkedList<>();

        tags = new HashMap<>();
        values = new HashMap<>();
        operations = new HashMap<>();

        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        logger.info("Listening on {}:{}", address, port);
        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port); //The port to bind to
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*-------------------- Register Message Serializers ------------------------------- */
        registerMessageSerializer(channelId, ReadTagMessage.MSG_ID, ReadTagMessage.serializer);
        registerMessageSerializer(channelId, ReadTagReplyMessage.MSG_ID, ReadTagReplyMessage.serializer);
        registerMessageSerializer(channelId, ReadReplyMessage.MSG_ID, ReadReplyMessage.serializer);
        registerMessageSerializer(channelId, WriteMessage.MSG_ID, WriteMessage.serializer);
        registerMessageSerializer(channelId, ACKMessage.MSG_ID, ACKMessage.serializer);

        /*-------------------- Register Message Handlers ------------------------------- */
        registerMessageHandler(channelId, ReadTagMessage.MSG_ID, this::uponReadTagMessage, this::uponMsgFail);
        registerMessageHandler(channelId, ReadTagReplyMessage.MSG_ID, this::uponReadTagReplyMessage, this::uponMsgFail);
        registerMessageHandler(channelId, WriteMessage.MSG_ID, this::uponWriteMessage, this::uponMsgFail);
        registerMessageHandler(channelId, ACKMessage.MSG_ID, this::uponACKMessage, this::uponMsgFail);
        registerMessageHandler(channelId, ReadReplyMessage.MSG_ID, this::uponReadReplyMessage, this::uponMsgFail);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ReadRequest.REQUEST_ID, this::uponReadRequest);
        registerRequestHandler(WriteRequest.REQUEST_ID, this::uponWriteRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
    }

    @Override
    public void init(Properties props) {
        //Inform the state machine protocol about the channel we created in the constructor
        triggerNotification(new ChannelReadyNotification(channelId, self));

        pending = null;

        String host = props.getProperty("initial_membership");
        String[] hosts = host.split(",");
        List<Host> initialMembership = new LinkedList<>();
        for (String s : hosts) {
            String[] hostElements = s.split(":");
            Host h;
            try {
                h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }
            initialMembership.add(h);
        }

        if (initialMembership.contains(self)) {
            logger.info("Starting in ACTIVE as I am part of initial membership");
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            
            processSequence = initialMembership.indexOf(self);
        } else {
            logger.info("Starting in JOINING as I am not part of initial membership");
        }
    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponWriteRequest(WriteRequest request, short sourceProto) {
        logger.info("Received WRITE request: " + request);

        nextInstance ++;
        pending = request.getData();
        answers = new LinkedList<>();

        String key = new String(request.getKey(), 0, request.getKey().length);
        operations.put(key, request.getOpId());
        tags.put(key, Pair.of(nextInstance, processSequence));
        
        membership.forEach(h -> sendMessage(new ReadTagMessage(nextInstance, key, false), h));
    } 

    private void uponReadRequest(ReadRequest request, short sourceProto) {
        logger.info("Received ReadRequest: " + request);

        pending = null;
        nextInstance ++;
        answers = new LinkedList<>();

        String key = new String(request.getKey(), 0, request.getKey().length);
        operations.put(key, request.getOpId());
        tags.put(key, Pair.of(nextInstance, processSequence));
        
        membership.forEach(h -> sendMessage(new ReadTagMessage(nextInstance, key, true), h));
    } 

    /*--------------------------------- Procedures ---------------------------------------- */
    private int maxSQTag(List<Pair<Pair<Integer, Integer>, byte[]>> ans) {
        Pair<Integer, Integer> max = Pair.of(0, 0);
        for (Pair<Pair<Integer, Integer>, byte[]> pair : ans) {
            Pair<Integer, Integer> h = pair.getLeft(); 
            if (h.getLeft() > max.getLeft() || 
                (h.getLeft() == max.getLeft() && h.getRight() > max.getRight()))
                max = Pair.of(h.getLeft(), h.getRight());    
        }

        return max.getLeft();
    }

    private Pair<Pair<Integer, Integer>, byte[]> maxTag(List<Pair<Pair<Integer, Integer>, byte[]>> ans) {
        Pair<Pair<Integer, Integer>, byte[]> max = Pair.of(Pair.of(0, 0), null);
        for (Pair<Pair<Integer, Integer>, byte[]> pair : ans) {
            Pair<Integer, Integer> h = pair.getLeft();
            Pair<Integer, Integer> m = max.getLeft(); 
            if (h.getLeft() > m.getLeft() || 
                (h.getLeft() == m.getLeft() && h.getRight() > m.getRight()))
                    max = pair;
        }

        return max;
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponReadTagMessage(ReadTagMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received ReadTagMessage: " + msg);

        Pair<Integer, Integer> ntag = tags.get(msg.getKey());
        if(ntag == null) {
            ntag = Pair.of(0, processSequence);
            tags.put(msg.getKey(), ntag);
        }

        if(msg.isReading()) {
            byte[] val =  values.get(msg.getKey()); 
            if (val == null) val = new byte[0];

            sendMessage(new ReadReplyMessage(msg.getOpSeq(), ntag, msg.getKey(), val), host);
        }else 
            sendMessage(new ReadTagReplyMessage(msg.getOpSeq(), ntag, msg.getKey()), host);
    }

    private void uponReadTagReplyMessage(ReadTagReplyMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received ReadTagReplyMessage: " + msg);

        if (nextInstance == msg.getOpId()) {
            if (pending != null)//After majority Decision, no more adds, or it can mess up ACK
                answers.add(Pair.of(msg.getTag(), null));
                
            if(answers.size() == (membership.size()/ 2) + 1 ) {
                int maxSQTag = maxSQTag(answers);
                answers = new LinkedList<>();
                nextInstance ++;
                membership.forEach(h -> {
                        sendMessage(new WriteMessage(
                            nextInstance, msg.getKey(), Pair.of(maxSQTag +1, processSequence), pending), h); 
                });
                
                pending = null;
            }
        }
    }

    private void uponReadReplyMessage(ReadReplyMessage msg, Host host, short sourceProto, int channelId) {
        logger.debug("Received ReadTagReplyMessage: " + msg);

        if (nextInstance == msg.getOpId()) {
            if (pending == null)//After majority Decision, no more adds, or it can mess up ACK
                answers.add(Pair.of(msg.getTag(), null));
                
            if(answers.size() == (membership.size()/ 2) + 1 ) {
                Pair<Pair<Integer, Integer>, byte[]> maxTag = maxTag(answers);
                Pair<Integer, Integer> tag = maxTag.getLeft();
                pending = (maxTag.getRight() == null) ? new byte[0] : maxTag.getRight();
                answers = new LinkedList<>();
                nextInstance ++;
                membership.forEach(h -> {
                        sendMessage(new WriteMessage(
                            nextInstance, msg.getKey(), tag, pending), h); 
                });
            }
        }
    }

    private void uponWriteMessage(WriteMessage msg, Host host, short sourceProto, int channelId) { 
        logger.debug("Received WriteMessage: INSTANCE {} - MSG: {} ", nextInstance, msg);

        Pair<Integer, Integer> tt = tags.get(msg.getKey());
        Pair<Integer, Integer> prevTT = tt;
        if ( (msg.getTag().getLeft() > tt.getLeft()) || (msg.getTag().getLeft() == tt.getLeft() 
                && msg.getTag().getRight() > tt.getRight()) ) {

            tags.put(msg.getKey(), msg.getTag());
            values.put(msg.getKey(), msg.getValue());
  
            if(self != host) {
                logger.info("Updated -> message: MSG: {} TAGMsg {} PrevTag {} ", msg, msg.getTag(), prevTT);
                triggerNotification(new UpdateValueNotification(msg.getOpId(), msg.getKey().getBytes(), msg.getValue()));
            }
        }

        sendMessage(new ACKMessage(msg.getOpId(), msg.getKey()), host);
    }

    private void uponACKMessage(ACKMessage msg, Host host, short sourceProto, int channelId) {
        if(nextInstance == msg.getOpId()) {//change ACK to send value
            answers.add(Pair.of(Pair.of(msg.getOpId(), processSequence), pending));

            if (answers.size() == (membership.size() / 2) + 1) {
                logger.info("NEW {}: opSeq {} - key {} - opId {}", (pending == null)? "WRITE" : "READ",
                                msg.getOpId(), msg.getKey(), operations.get(msg.getKey()));

                answers = new LinkedList<>();
                if (pending == null) {      
                    triggerNotification(new WriteCompleteNotification(
                        nextInstance, msg.getKey().getBytes(), values.get(msg.getKey()), operations.get(msg.getKey())));
                } else {                    
                    triggerNotification(new ReadCompleteNotification(
                        nextInstance, msg.getKey().getBytes(), pending, operations.get(msg.getKey())));
                }
                    
            }
        }
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());
        //Maybe we don't want to do this forever. At some point we assume he is no longer there.
        //Also, maybe wait a little bit before retrying, or else you'll be trying 1000s of times per second
        if(membership.contains(event.getNode()))
            openConnection(event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

}
