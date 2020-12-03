mkdir -p /home/hadoop/logs/
mkdir -p /home/hadoop/scripts/

aws s3 cp s3://umsstats-dev/usersvcLog/scripts /home/hadoop/scripts/ --recursive