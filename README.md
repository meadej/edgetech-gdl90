# edgetech-gdl90
An edgetech module developed to read GDL90 Heartbeat and Traffic Report messages incoming from a Stratux RTL-SDR UAT sensor and place it on to an associated MQTT bus.

## Operation
The edgetech-gdl90 module can be run using the included docker-compose file and environment variables. For **most** purposes, the default GDL receive host of `0.0.0.0` is sufficient.

Note that receiving on any port below 1024 typically requires elevated permissions. 
