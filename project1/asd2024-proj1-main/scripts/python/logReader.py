import re
import os

send_pattern = r'Sending:\s(-\w{64})'  
receive_pattern = r'Received\s(\w{64})\s->\s(-\w{64})'  

p2p_receive_pattern = r'Received Point2PointMessage: Point2PointMessage\{mid=(-\w{36})'

messages_sent = []
app_messages_received = []
p2p_messages_received = []

log_file_path = os.path.join(os.getcwd(), 'console.log')

if not os.path.exists(log_file_path):
    print(f"No log file found at {log_file_path}")
else:
    with open(log_file_path) as f:
        for line in f:
            if "Sending" in line:
                match = re.search(send_pattern, line)
                if match:
                    msg_id = match.group(1)
                    messages_sent.append({
                        'id': msg_id
                    })
            elif "Received" in line and "(1024)" in line:
                match = re.search(receive_pattern, line)
                if match:
                    received_id = match.group(1)
                    ack_id = match.group(2)
                    app_messages_received .append({
                        'received_id': received_id,
                        'ack_id': ack_id
                    })
            elif "Received Point2PointMessage" in line:
                p2p_messages_received.append({'mid': "fuck you"})
                # Extract message ID for regular Point2PointMessage



total_messages_sent = len(messages_sent)
total_messages_received_app = len(app_messages_received )
total_messages_received_p2p = len(p2p_messages_received)
redundancy = total_messages_received_app / total_messages_sent if total_messages_sent > 0 else 0

print("Messages Sent:")
for msg in messages_sent:
    print(msg)

print("\nMessages Received:")
for msg in app_messages_received:
    print(msg)

print("\nMessages Received from P2P:")
for msg in p2p_messages_received:
    print(msg)

print(f"\nTotal Messages Sent: {total_messages_sent}")
print(f"Total Messages Received from Application: {total_messages_received_app}")
print(f"Total Messages Received from P2P: {total_messages_received_p2p}")
print(f"Redundancy Ratio (App): {redundancy:.2f}")
