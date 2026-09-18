.PHONY: run

run:
	STREAM_SASL_MECHANISM="SCRAM-SHA-512" \
	STREAM_SECURITY_PROTOCOL="SASL_PLAINTEXT" \
	STREAM_SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required username="shared-service-user" password="CRvl4k3Pt1nMBfhQAoIAxdJ4emBpF8mI";' \
	./gradlew run --args="-c radar.yml stream"

remove-stream:
	rm -rf /tmp/kafka-streams/