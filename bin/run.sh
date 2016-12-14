#!/usr/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
JAVA=${JAVA_HOME}/bin/java
YARN_CLASSPATH="${CLASSPATH}"
export CLASSPATH="${CLASSPATH}:${YARN_CLASSPATH}"

echo "./run.sh " $@

CONTAINER_TYPE=$1
shift


if [[ $CONTAINER_TYPE == 'application-master' ]]; then
	echo "Launching Application Master"
	"${JAVA}" -cp "${CLASSPATH}" \
                -Dlogger.type=Console \
                -Xmx256M \
                com.iwantfind.ApplicationMaster $@


elif [[ $CONTAINER_TYPE == 'worker' ]]; then
	exec $@
else 
   echo "Unrecognized container type: $CONTAINER_TYPE" >&2
   exit 1
fi

