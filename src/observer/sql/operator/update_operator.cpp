/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by ZhangCL on 2022/10/13.
//

#include "common/log/log.h"
#include "sql/operator/update_operator.h"
#include "storage/record/record.h"
#include "storage/common/table.h"
#include "sql/stmt/update_stmt.h"

RC UpdateOperator::open()
{
  RC rc = RC::SUCCESS;
  if (children_.size() != 1) {
    LOG_WARN("update operator must has 1 child");
    return RC::INTERNAL;
  }

  Operator *child = children_[0];
  rc = child->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  Table *table = update_stmt_->table();
  const TableMeta &table_meta = table->table_meta();
  int record_size = table_meta.record_size();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple * tuple = child->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get current record. rc=%s", strrc(rc));
      break;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &record = row_tuple->record();
    if (0 == memcmp(record.data() + update_stmt_->field_offset(), 
                      update_stmt_->value()->data, update_stmt_->field_length())) {
      LOG_WARN("duplicate value");
      return  RC::RECORD_DUPLICATE_KEY;
    }

    char *old_data = record.data();
    char *data = new char[record_size];       // new_record->data
    memcpy(data, old_data, record_size);
    memcpy(data + update_stmt_->field_offset(), update_stmt_->value()->data, update_stmt_->field_length());
    record.set_data(data);
    // if (0 == memcmp(old_data, record.data(), record_size))
    // {
    //   LOG_ERROR("change data failed");
    //   return RC::GENERIC_ERROR;
    // }

    // check whether need to update index 


    rc = table->update_record(nullptr, &record, old_data, update_stmt_->field_is_index());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      return rc;
    }
    update_row_num_++;
  }
  if (RC::RECORD_EOF != rc) {
    return rc;
  }

  return RC::SUCCESS;
}

RC UpdateOperator::next()
{
  return RC::RECORD_EOF;
}

RC UpdateOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}