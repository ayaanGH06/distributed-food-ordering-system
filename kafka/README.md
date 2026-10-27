# Kafka cluster configuration templates

These are templates for the two-broker KRaft cluster required by the assignment.

VM2 is both the controller and Broker 1. VM3 is Broker 2 only. Replace the placeholder IPs with the VMs' ZeroTier IPs before starting Kafka.

For Kafka 3.9.x, format the configs as standard `.properties` files and use the Kafka `kafka-storage.sh` tooling to format a fresh log directory with the same cluster ID on both brokers.

**Important:** Do not blindly overwrite an existing Kafka installation or log directory. Follow the team's existing Kafka installation path and back up any existing server configuration first.
