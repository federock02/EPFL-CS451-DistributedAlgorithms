import sys
from collections import defaultdict

def check_fifo_order(log_file):
    # List to track broadcast message IDs
    broadcast_logs = []
    # Dictionary to track delivery message IDs for each senderId
    delivery_logs = defaultdict(list)

    with open(log_file, 'r') as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) == 2 and parts[0] == "b":
                # Broadcast log entry: "b messageId"
                message_id = int(parts[1])
                broadcast_logs.append(message_id)
            elif len(parts) == 3 and parts[0] == "d":
                # Delivery log entry: "d senderId messageId"
                sender_id = int(parts[1])
                message_id = int(parts[2])
                delivery_logs[sender_id].append(message_id)
            else:
                print(f"Invalid log entry: {line.strip()}")

    # Check if broadcast message IDs are in order
    print("Checking broadcast order:")
    if broadcast_logs != sorted(broadcast_logs):
        print(f"Out-of-order broadcast messages: {broadcast_logs}")
    else:
        print("Broadcast messages are in order.")

    # Check if message IDs are in order for deliveries
    print("\nChecking delivery order:")
    for sender_id, messages in delivery_logs.items():
        if messages != sorted(messages):
            print(f"Out-of-order messages for sender {sender_id}: {messages}")
        else:
            print(f"Messages in order for sender {sender_id}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_fifo_order.py <log_file>")
        sys.exit(1)

    log_file_path = sys.argv[1]
    check_fifo_order(log_file_path)
