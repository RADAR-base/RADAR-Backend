#!/bin/bash

# Check if variables exist
if [ -z "$KAFKA_REST_PROXY" ]; then
        echo "KAFKA_REST_PROXY is not defined"
        exit 126
fi

if [ -z "$TOPIC_LIST" ]; then
        echo "TOPIC_LIST is not defined"
        exit 126
fi

# Fetch env topic list
IFS=', ' read -r -a needed <<< $TOPIC_LIST

# Fetch env topic list
count=0
interval=1
while [ "$count" != "${#needed[@]}" ] ; do

    echo "Waiting $interval second before retrying ..."
    sleep $interval
    if (( interval < 30 )); then
        ((interval=interval*2))
    fi

    count=0
    TOPICS=$(curl -sSX GET -H "Content-Type: application/json" "$KAFKA_REST_PROXY/topics")
    TOPICS="$(echo -e "${TOPICS}" | tr -d '"'  | tr -d '['  | tr -d ']' | tr -d '[:space:]' )"

    IFS=',' read -r -a array <<< $TOPICS
    for topic in "${array[@]}"
    do
        for need in "${needed[@]}"
        do
            if [ "$topic" = "$need" ] ; then
                ((count++))
            fi
        done
    done
done

echo "All topics are now available. Ready to go!"