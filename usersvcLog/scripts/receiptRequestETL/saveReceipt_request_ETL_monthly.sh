INPUT_DIR=$1
OUTPUT_DIR=$2
IS_LOCAL_RUNNING=$3

echo "INPUT_DIR: ${INPUT_DIR}"
echo "OUTPUT_DIR: ${OUTPUT_DIR}"
echo "IS_LOCAL_RUNNING: ${IS_LOCAL_RUNNING}"

spark-submit \
--master yarn \
--deploy-mode cluster \
--class SaveReceiptRequest.SaveReceiptRequestETLMonthly \
/home/hadoop/scripts/usersvcLog-1.0-SNAPSHOT.jar \
${INPUT_DIR} \
${OUTPUT_DIR} \
${IS_LOCAL_RUNNING}