package protocols.point2point;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.chord.notifications.TCPChannelCreatedNotification;
import protocols.dht.chord.replies.LookupReply;
import protocols.dht.chord.requests.LookupRequest;
import protocols.point2point.messages.HelperNodeMessage;
import protocols.point2point.messages.P2PReceivedMessage;
import protocols.point2point.messages.Point2PointMessage;
import protocols.point2point.notifications.DHTInitializedNotification;
import protocols.point2point.notifications.Deliver;
import protocols.point2point.requests.Send;
import protocols.point2point.timers.HelperTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.*;

public class Point2PointCommunicator extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(Point2PointCommunicator.class);

	public final static String PROTOCOL_NAME = "EmptyPoint2PointComm";
	public final static short PROTOCOL_ID = 400;

	private final Host thisHost;
	private final short DHT_PROTO_ID;

	private int tcpChannelId;

	private final List<Send> messagesPendingLookup;
	private final Map<UUID, Send> messagesPendingLookupReply;
	private final Set<UUID> receivedMessages;
	private final Set<HelperNodeMessage> helperMessagesToSend; //HelperNodeMessages I received as a helper
	private final Map<Host, Set<HelperNodeMessage>> myHelpersMessages; //HelperNodeMessages I sent to helpers

	private boolean isDHTInitialized;

	public Point2PointCommunicator(Host thisHost, short DHT_Proto_ID) throws HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.thisHost = thisHost;
		this.DHT_PROTO_ID = DHT_Proto_ID;
		this.messagesPendingLookup = new LinkedList<>();
		this.messagesPendingLookupReply = new HashMap<>();
		this.receivedMessages = new HashSet<>();
		this.helperMessagesToSend = new HashSet<>();
		this.myHelpersMessages = new HashMap<>();
		this.isDHTInitialized = false;

		//register request handlers
		registerRequestHandler(Send.REQUEST_ID, this::uponSendRequest);

		//register reply handlers
		registerReplyHandler(LookupReply.REPLY_ID, this::uponLookupReply);

		//register notification handlers
		subscribeNotification(TCPChannelCreatedNotification.NOTIFICATION_ID, this::uponChannelCreated);
		subscribeNotification(DHTInitializedNotification.NOTIFICATION_ID, this::uponDHTInitialized);

		//register timer handlers
		registerTimerHandler(HelperTimer.TIMER_ID, this::helperTimer);
	}

	@Override
	public void init(Properties props) {
		setupPeriodicTimer(new HelperTimer(), 3000, 3000);
	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponSendRequest(Send request, short protoID) {
		logger.info("Received SendRequest: {}", request.getMessageID());

		if (!isDHTInitialized) {
			logger.info("DHT protocol is not initialized yet, queueing message: {}", request.getMessageID());
			messagesPendingLookup.add(request);
			return;
		}

		LookupRequest lookupRequest = new LookupRequest(request.getDestinationPeerID(), request.getMessageID());
		sendRequest(lookupRequest, DHT_PROTO_ID);
		messagesPendingLookupReply.put(request.getMessageID(), request);
	}

	/*--------------------------------- Messages ---------------------------------------- */

	private void uponPoint2PointMessage(Point2PointMessage point2PointMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received Point2Point Message: {}", point2PointMessage.toString());

		if (receivedMessages.contains(point2PointMessage.getMid())) {
			//TODO P2PReceivedMessage (Name of the msg)
			//New msg to sender to ask him to remove from his list this message
			logger.info("I AM DUPLICATED");
			HelperNodeMessage msg = new HelperNodeMessage(point2PointMessage);
			logger.info("{} ", point2PointMessage.getSender());

			InetSocketAddress adjustedSender = new InetSocketAddress(point2PointMessage.getSender().getAddress(), point2PointMessage.getSender().getPort() - 1);

			openConnection(point2PointMessage.getSender());
			sendMessage(new P2PReceivedMessage(msg), point2PointMessage.getSender());		

			logger.info(point2PointMessage.getSender());

			
			return;
		}
		triggerNotification(new Deliver(point2PointMessage));
		receivedMessages.add(point2PointMessage.getMid());
	}

	private void uponHelperNodeMessage(HelperNodeMessage msg, Host from, short sourceProto, int channelId) {
		helperMessagesToSend.add(msg);

		Point2PointMessage p2pMsg = new Point2PointMessage(msg);
		logger.info("HELPER {} IS ALSO SENDING THE MESSAGE {} TO ---> {}", thisHost, p2pMsg.getMid(), p2pMsg.getDestination());
		openConnection(p2pMsg.getDestination());
		sendMessage(p2pMsg, p2pMsg.getDestination());
	}

	private void uponP2PReceivedMessage(P2PReceivedMessage p2pReceivedMsg, Host from, short sourceProto, int channelId) {
		logger.info("Received Warning of Duplicated Message: {}", p2pReceivedMsg.toString());

		//receivedMessages.add(p2pReceivedMsg.getMid());
		logger.info("HERE");
		helperMessagesToSend.remove(new HelperNodeMessage(p2pReceivedMsg));
	}

	

	private void uponHelperMessageFail(HelperNodeMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);


		//select the predecessor of helper as the next helper.

		//	HelperNodeMessage helperNodeMessage = new HelperNodeMessage(msg);
		//TODO
		//Change impl to, forget the pre-predecessor
		//We just store the succ and sender, when sender goes down(not suc the one returned p2p)
		//if the helper goes down, we simply need to lookUp the DHT table for a new one for that key.
		//Easier
/* 		openConnection(msg.getPreHelper());
		sendMessage(msg, msg.getPreHelper()); */
	}

	private void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);

		//select the predecessor of helper as the next helper.

		//	HelperNodeMessage helperNodeMessage = new HelperNodeMessage(msg);
	}

	/*--------------------------------- Notifications ---------------------------------------- */
	private void uponLookupReply(LookupReply reply, short protoID) {
		logger.info("Received Lookup Reply: {}", reply.toString());

		Send send = messagesPendingLookupReply.get(reply.getMid());
		if (send == null) return;

		Iterator<Pair<BigInteger, Host>> it = reply.getPeersIterator();
		Pair<BigInteger, Host> helper = it.next();
		Host successorHost = it.next().getRight();

		Point2PointMessage point2PointMessage = new Point2PointMessage(send, thisHost, successorHost);
		if (successorHost.equals(thisHost)) {
			uponPoint2PointMessage(point2PointMessage, successorHost, PROTOCOL_ID, tcpChannelId);
			return;
		}

		logger.info("SENDING PAYLOAD TO ---> {}", successorHost);
		openConnection(successorHost);
		sendMessage(point2PointMessage, successorHost);
		messagesPendingLookupReply.remove(reply.getMid());

		logger.info("SENDING DUP MESSAGE TO HELPER ---> {}", helper.getRight());
		if (!helper.getRight().equals(successorHost)) {
			HelperNodeMessage helperNodeMessage = new HelperNodeMessage(point2PointMessage);
			openConnection(helper.getRight());
			sendMessage(helperNodeMessage, helper.getRight());
		}
	}

	//Upon receiving the channelId from the DHT algorithm, register our own callbacks and serializers
	private void uponChannelCreated(TCPChannelCreatedNotification notification, short sourceProto) {
		logger.info("TCPChannelCreatedNotification: {}", notification.toString());

		tcpChannelId = notification.getChannelId();
		registerSharedChannel(tcpChannelId);

		//register message serializers
		registerMessageSerializer(tcpChannelId, Point2PointMessage.MSG_ID, Point2PointMessage.serializer);
		registerMessageSerializer(tcpChannelId, HelperNodeMessage.MSG_ID, HelperNodeMessage.serializer);
		registerMessageSerializer(tcpChannelId, P2PReceivedMessage.MSG_ID, P2PReceivedMessage.serializer);

		//register message handlers
		try {
			registerMessageHandler(tcpChannelId, Point2PointMessage.MSG_ID, this::uponPoint2PointMessage, this::uponMessageFail);
			registerMessageHandler(tcpChannelId, HelperNodeMessage.MSG_ID, this::uponHelperNodeMessage, this::uponHelperMessageFail);
			registerMessageHandler(tcpChannelId, P2PReceivedMessage.MSG_ID, this::uponP2PReceivedMessage, this::uponMessageFail);
		} catch (HandlerRegistrationException e) {
			logger.error("Error registering message handler: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void uponDHTInitialized(DHTInitializedNotification notification, short sourceProto) {
		logger.info("DHT protocol initialized.");

		isDHTInitialized = true;
		for (Send pendingMessage : messagesPendingLookup) {
			uponSendRequest(pendingMessage, PROTOCOL_ID);
		}
		messagesPendingLookup.clear();
	}

	/*--------------------------------- Timers ---------------------------------------- */

	private void helperTimer(HelperTimer timer, long timerId) {
		logger.debug("helperTimer: {}", timerId);

		for (HelperNodeMessage helperNodeMessage : helperMessagesToSend) {
			sendMessage(new Point2PointMessage(helperNodeMessage), helperNodeMessage.getDestination());
		}
	}

}
