INPUT_DIR=$1
OUTPUT_DIR=$2
YEAR_MONTH=$3
IS_LOCAL_RUNNING=$4

echo "INPUT_DIR: ${INPUT_DIR}"
echo "OUTPUT_DIR: ${OUTPUT_DIR}"
echo "YEAR_MONTH: ${YEAR_MONTH}"
echo "IS_LOCAL_RUNNING: ${IS_LOCAL_RUNNING}"

spark-submit \
--master yarn \
--deploy-mode cluster \
--class userlog.UserLogMonthlyBriefDataProcess \
/home/hadoop/scripts/usersvcLog-1.0-SNAPSHOT.jar \
${INPUT_DIR} \
${OUTPUT_DIR} \
${YEAR_MONTH} \
${IS_LOCAL_RUNNING}