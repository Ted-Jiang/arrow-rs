// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Logic for reading into arrow arrays

use crate::errors::Result;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType as ArrowType;
use std::any::Any;
use std::sync::Arc;

use crate::arrow::record_reader::buffer::ValuesBuffer;
use crate::arrow::record_reader::GenericRecordReader;
use crate::column::page::PageIterator;
use crate::column::reader::decoder::ColumnValueDecoder;
use crate::file::reader::{FilePageIterator, FileReader};
use crate::schema::types::SchemaDescPtr;

mod builder;
mod byte_array;
mod byte_array_dictionary;
mod complex_object_array;
mod empty_array;
mod list_array;
mod map_array;
mod null_array;
mod primitive_array;
mod struct_array;

#[cfg(test)]
mod test_util;

use crate::file::page_index::filer_offset_index::FilterOffsetIndex;
pub use builder::build_array_reader;
pub use byte_array::make_byte_array_reader;
pub use byte_array_dictionary::make_byte_array_dictionary_reader;
pub use complex_object_array::ComplexObjectArrayReader;
pub use list_array::ListArrayReader;
pub use map_array::MapArrayReader;
pub use null_array::NullArrayReader;
pub use primitive_array::PrimitiveArrayReader;
pub use struct_array::StructArrayReader;

/// Array reader reads parquet data into arrow array.
pub trait ArrayReader: Send {
    fn as_any(&self) -> &dyn Any;

    /// Returns the arrow type of this array reader.
    fn get_data_type(&self) -> &ArrowType;

    /// Reads at most `batch_size` records into an arrow array and return it.
    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef>;

    /// If this array has a non-zero definition level, i.e. has a nullable parent
    /// array, returns the definition levels of data from the last call of `next_batch`
    ///
    /// Otherwise returns None
    ///
    /// This is used by parent [`ArrayReader`] to compute their null bitmaps
    fn get_def_levels(&self) -> Option<&[i16]>;

    /// If this array has a non-zero repetition level, i.e. has a repeated parent
    /// array, returns the repetition levels of data from the last call of `next_batch`
    ///
    /// Otherwise returns None
    ///
    /// This is used by parent [`ArrayReader`] to compute their array offsets
    fn get_rep_levels(&self) -> Option<&[i16]>;
}

/// A collection of row groups
pub trait RowGroupCollection {
    /// Get schema of parquet file.
    fn schema(&self) -> Result<SchemaDescPtr>;

    /// Get the numer of rows in this collection
    fn num_rows(&self) -> usize;

    /// Returns an iterator over the column chunks for particular column
    /// 'row_groups_filter_offset_index' is optional for reducing useless IO
    /// by filtering needless page.
    fn column_chunks(
        &self,
        i: usize,
        _row_groups_filter_offset_index: Option<&Vec<Vec<FilterOffsetIndex>>>,
    ) -> Result<Box<dyn PageIterator>>;
}

impl RowGroupCollection for Arc<dyn FileReader> {
    fn schema(&self) -> Result<SchemaDescPtr> {
        Ok(self.metadata().file_metadata().schema_descr_ptr())
    }

    fn num_rows(&self) -> usize {
        self.metadata().file_metadata().num_rows() as usize
    }

    fn column_chunks(
        &self,
        i: usize,
        row_groups_filter_offset_index: Option<&Vec<Vec<FilterOffsetIndex>>>,
    ) -> Result<Box<dyn PageIterator>> {
        let iterator = FilePageIterator::new(
            i,
            Arc::clone(self),
            row_groups_filter_offset_index.cloned(),
        )?;
        Ok(Box::new(iterator))
    }
}

/// Uses `record_reader` to read up to `batch_size` records from `pages`
///
/// Returns the number of records read, which can be less than batch_size if
/// pages is exhausted.
fn read_records<V, CV>(
    record_reader: &mut GenericRecordReader<V, CV>,
    pages: &mut dyn PageIterator,
    batch_size: usize,
) -> Result<usize>
where
    V: ValuesBuffer + Default,
    CV: ColumnValueDecoder<Slice = V::Slice>,
{
    let mut records_read = 0usize;
    while records_read < batch_size {
        let records_to_read = batch_size - records_read;

        let records_read_once = record_reader.read_records(records_to_read)?;
        records_read += records_read_once;

        // Record reader exhausted
        if records_read_once < records_to_read {
            if let Some(page_reader) = pages.next() {
                // Read from new page reader (i.e. column chunk)
                record_reader.set_page_reader(page_reader?)?;
            } else {
                // Page reader also exhausted
                break;
            }
        }
    }
    Ok(records_read)
}
