#!/bin/sh

# As google forces you to click the button to download, we have to get a little
# clever. Basically we extract the confirmation key, then make another request
# while keeping the session.
download() {
    base="https://drive.google.com/uc?export=download&id=$1"
    key=`curl -s -c cookie.txt "$base" | grep -Po "confirm=.*?&" | sed "s/&//; s/.*=//"`
    curl -L -b cookie.txt "$base&confirm=$key" > $2
    rm cookie.txt
}

cd /mnt

download "0B-HgS_aEZYWSUWJLS2h1cWtMMFE" "emails.csv"
download "0B-HgS_aEZYWSell3Ung3M2RMOWs" "emails_preprocessed.csv"

python3 -m "nltk.downloader" "words"

echo "Wait till cluster is finished and then press 'Enter'"
read

hadoop fs -put emails.csv emails_preprocessed.csv /user/hadoop/

sudo yum install -y perf htop vim
sudo pip-3.4 install mrjob
