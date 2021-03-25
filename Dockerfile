FROM confluentinc/cp-kafka-connect-base:5.3.1

COPY ./config/* /kafka-connect-reddit/
COPY dockerStart.sh /kafka-connect-reddit/
COPY target/components/packages/* /kafka-connect-reddit/install/

ENTRYPOINT ["/kafka-connect-reddit/dockerStart.sh"]
