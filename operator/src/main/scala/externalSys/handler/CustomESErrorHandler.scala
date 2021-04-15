package externalSys.handler

import org.elasticsearch.hadoop.handler.HandlerResult
import org.elasticsearch.hadoop.rest.bulk.handler.{BulkWriteErrorHandler, BulkWriteFailure, DelayableErrorCollector}

class CustomESErrorHandler extends BulkWriteErrorHandler{
  override def onError(entry: BulkWriteFailure, collector: DelayableErrorCollector[Array[Byte]]): HandlerResult = {
    HandlerResult.HANDLED
  }
}
