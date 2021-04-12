INPUT_DIR=$1
OUTPUT_DIR=$2
DATE=$3
IS_LOCAL_FILE=$4
IS_LOCAL_RUNNING=$5

echo "INPUT_DIR: ${INPUT_DIR}"
echo "OUTPUT_DIR: ${OUTPUT_DIR}"
echo "DATE: ${DATE}"
echo "IS_LOCAL_FILE: ${IS_LOCAL_FILE}"
echo "IS_LOCAL_RUNNING: ${IS_LOCAL_RUNNING}"

spark-submit \
--master yarn \
--deploy-mode cluster \
--class SGLReceipts.SGLReceiptsFilter \
/home/hadoop/scripts/usersvcLog-1.0-SNAPSHOT.jar \
${INPUT_DIR} \
${OUTPUT_DIR} \
${DATE} \
${IS_LOCAL_FILE} \
${IS_LOCAL_RUNNING}