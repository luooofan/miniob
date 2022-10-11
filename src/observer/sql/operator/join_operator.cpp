
//create by yangjk b [select tables]
#include "sql/operator/join_operator.h"
#include "storage/common/table.h"
#include "rc.h"

RC JoinOperator::open()
{
  RC rc = RC::SUCCESS;
  if((RC::SUCCESS != (rc = left_->open())) && (RC::SUCCESS != (rc = right_->open())))
  {
    rc = RC::INTERNAL;
    LOG_WARN("JoinOperater child open failed!");
  }
  Tuple * left_tuple = left_->current_tuple();
  Tuple * right_tuple = right_->current_tuple();
  tuple_.init(left_tuple, right_tuple);
  return rc;
}

RC JoinOperator::next()
{
  RC rc = RC::SUCCESS;
  if (RC::SUCCESS != (rc = left_->next())) {
    if (RC::RECORD_EOF == rc) {
      return RC::RECORD_EOF;
    }
  }
  if (RC::SUCCESS != (rc = right_->next())) {

  }
  // Tuple* left_tup = left_->current_tuple();
  // Tuple* right_tup = right_->current_tuple();
  return rc;
}

RC JoinOperator::close()
{
  RC rc = RC::SUCCESS;
  left_->close();
  right_->close();
  return rc;
}

Tuple * JoinOperator::current_tuple()
{
  return &tuple_;
}