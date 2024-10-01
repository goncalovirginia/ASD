package protocols.broadcast.gossip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.gossip.messages.GossipMessage;
import protocols.broadcast.gossip.messages.LazyGossipMessage;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.*;

public class LazyPushGossipBroadcast extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(LazyPushGossipBroadcast.class);

	//Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "LazyPushGossip";
	public static final short PROTOCOL_ID = 500;

	private final Host myself;
	private final Set<Host> neighbours;
	private final Map<UUID, GossipMessage> receivedMessages;
	private boolean channelReady;

	public LazyPushGossipBroadcast(Properties properties, Host myself) throws HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		neighbours = new HashSet<>();
		receivedMessages = new HashMap<>();
		channelReady = false;

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*--------------------- Register Notification Handlers ----------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
	}

	@Override
	public void init(Properties props) {

	}

	/*--------------------------------- Requests ---------------------------------------- */
	private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
		if (!channelReady) return; //Ideally we would buffer this message to transmit when the channel is ready :)

		//Create the message object
		GossipMessage msg = new GossipMessage(request.getMsgId(), request.getSender(), sourceProto, request.getMsg());

		//Call the same handler as when receiving a new GossipMessage (since the logic is the same)
		uponGossipMessage(msg, myself, getProtoId(), -1);
	}

	/*--------------------------------- Messages ---------------------------------------- */
	private void uponGossipMessage(GossipMessage msg, Host from, short sourceProto, int channelId) {
		logger.info("Received {} from {}", msg, from);

		//If we already received it once, do nothing (or we would end up with a nasty infinite loop)
		if (receivedMessages.putIfAbsent(msg.getMid(), msg) != null) return;

		//Deliver the message to the application (even if it came from it)
		triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent()));

		//Create the lazy gossip message object to query the randomized neighbouring processes
		LazyGossipMessage lazyGossipMessage = new LazyGossipMessage(msg.getMid(), msg.getSender(), sourceProto, LazyGossipMessage.LazyGossipAction.PUSH);

		//Lazy push the message to ceil(ln(neighbours.size() + 1)) neighbours (who will then do the same)
		for (Host neighbour : neighboursShuffleLnSublist(from)) {
			sendMessage(lazyGossipMessage, neighbour);
			logger.info("Sent Lazy Gossip PUSH {} to {}", msg, neighbour);
		}
	}

	private void uponLazyGossipMessage(LazyGossipMessage msg, Host from, short sourceProto, int channelId) {
		logger.info("Received {} from {}", msg, from);

		switch (msg.getAction()) {
			case PUSH:
				processLazyGossipMessagePush(msg, from, sourceProto, channelId);
				break;
			case PULL:
				processLazyGossipMessagePull(msg, from, sourceProto, channelId);
				break;
			default:
				logger.error("Invalid Lazy Gossip Message action {} from {}", msg, from);
		}
	}

	private List<Host> neighboursShuffleLnSublist(Host from) {
		List<Host> neighboursList = new ArrayList<>(neighbours);
		neighboursList.remove(from);
		Collections.shuffle(neighboursList);
		return neighboursList.subList(0, (int) Math.ceil(Math.log(neighboursList.size() + 1)));
	}

	private void processLazyGossipMessagePush(LazyGossipMessage msg, Host from, short sourceProto, int channelId) {
		//If we already received the message once, we don't need to PULL it again
		if (receivedMessages.containsKey(msg.getMid())) return;

		//Send PULL message back to the sender, requesting the actual GossipMessage with the contents
		LazyGossipMessage lazyGossipMessage = new LazyGossipMessage(msg.getMid(), myself, sourceProto, LazyGossipMessage.LazyGossipAction.PULL);
		sendMessage(lazyGossipMessage, from);
		logger.info("Sent Lazy Gossip PULL {} to {}", msg, from);
	}

	private void processLazyGossipMessagePull(LazyGossipMessage msg, Host from, short sourceProto, int channelId) {
		//Check if the requested message doesn't exist, just in case, in order to avoid a potential error
		GossipMessage requestedMessage = receivedMessages.get(msg.getMid());
		if (requestedMessage == null) return;

		//Send full GossipMessage back to the sender that pulled it
		sendMessage(requestedMessage, from);
		logger.info("Sent {} to {}", requestedMessage, from);
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		//If a message fails to be sent, for whatever reason, log the message and the reason
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Notifications ---------------------------------------- */

	//When the membership protocol notifies of a new neighbour, simply update my list of neighbours
	private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.add(h);
			logger.info("New neighbour: {}", h);
		}
	}

	//When the membership protocol notifies of a leaving neighbour simply update my list of neighbours
	private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
		for (Host h : notification.getNeighbours()) {
			neighbours.remove(h);
			logger.info("Neighbour down: {}", h);
		}
	}

	//Upon receiving the channelId from the membership, register our own callbacks and serializers
	private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
		int channelId = notification.getChannelId();
		// Allows this protocol to receive events from this channel.
		registerSharedChannel(channelId);
		/*---------------------- Register Message Serializers ---------------------- */
		registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);
		registerMessageSerializer(channelId, LazyGossipMessage.MSG_ID, LazyGossipMessage.serializer);
		/*---------------------- Register Message Handlers -------------------------- */
		try {
			registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponGossipMessage, this::uponMsgFail);
			registerMessageHandler(channelId, LazyGossipMessage.MSG_ID, this::uponLazyGossipMessage, this::uponMsgFail);
		} catch (HandlerRegistrationException e) {
			logger.error("Error registering message handler: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
		//Now we can start sending messages
		channelReady = true;
	}

	public void printReceivedMessages() {
		for (UUID uuid : receivedMessages.keySet()) {
			System.out.println(uuid);
		}
	}

}
