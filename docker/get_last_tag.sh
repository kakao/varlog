UNAME=$(uname -s)

REPO_ID=${1}
DOCKER=docker
if [ "$UNAME" != "Darwin" ]; then
    DOCKER="sudo docker"
fi

ILIST=$($DOCKER images | grep "${REPO_ID} " | egrep -v "latest|alpha|TAG");
echo $(echo "$ILIST" | awk '{print $2}' | head -n1)
