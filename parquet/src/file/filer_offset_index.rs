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

use crate::file::page_index::range::{Range, RowRanges};
use parquet_format::PageLocation;

/// Returns the filtered offset index containing only the pages which are overlapping with rowRanges.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilterOffsetIndex {
    // read from parquet file which before the footer.
    offset_index: Vec<PageLocation>,

    // use to keep needed page index.
    index_map: Vec<usize>,
}

pub(crate) type OffsetRange = (Vec<usize>, Vec<usize>);

impl FilterOffsetIndex {
    pub(crate) fn try_new(
        offset_index: &[PageLocation],
        ranges: &RowRanges,
        total_row_count: i64,
    ) -> Self {
        let mut index = vec![];
        for i in 0..offset_index.len() {
            let page_location: &PageLocation = &offset_index[i];
            let page_range = if i == offset_index.len() - 1 {
                Range::new(
                    page_location.first_row_index as usize,
                    total_row_count as usize,
                )
            } else {
                let next_page_location: &PageLocation = &offset_index[i + 1];
                Range::new(
                    page_location.first_row_index as usize,
                    (next_page_location.first_row_index - 1) as usize,
                )
            };
            if ranges.is_overlapping(&page_range) {
                index.push(i);
            }
        }

        FilterOffsetIndex {
            offset_index: offset_index.to_vec(),
            index_map: index,
        }
    }

    pub(crate) fn get_page_count(&self) -> usize {
        self.index_map.len()
    }

    pub(crate) fn get_offset(&self, page_index: usize) -> i64 {
        let index = self.index_map[page_index];
        self.offset_index.get(index as usize).unwrap().offset
    }

    pub(crate) fn get_compressed_page_size(&self, page_index: usize) -> i32 {
        let index = self.index_map[page_index];
        self.offset_index
            .get(index as usize)
            .unwrap()
            .compressed_page_size
    }

    pub(crate) fn get_first_row_index(&self, page_index: usize) -> i64 {
        let index = self.index_map[page_index];
        self.offset_index
            .get(index as usize)
            .unwrap()
            .first_row_index
    }

    pub(crate) fn get_last_row_index(
        &self,
        page_index: usize,
        total_row_count: i64,
    ) -> i64 {
        let next_index = self.index_map[page_index] + 1;
        if next_index >= self.get_page_count() {
            total_row_count
        } else {
            self.offset_index
                .get(next_index as usize)
                .unwrap()
                .first_row_index
                - 1
        }
    }

    // Return the offset of needed both data page and dictionary page.
    // need input `row_group_offset` as input for checking if there is one dictionary page
    // in one column chunk.
    pub(crate) fn calculate_offset_range(&self, row_group_offset: i64) -> OffsetRange {
        let mut start_list = vec![];
        let mut length_list = vec![];
        let page_count = self.get_page_count();
        if page_count > 0 {
            let first_page_offset = self.get_offset(0);
            // add dictionary page if required
            if row_group_offset < first_page_offset {
                start_list.push(row_group_offset as usize);
                length_list.push((first_page_offset - 1) as usize);
            }
            let mut current_offset = self.get_offset(0);
            let mut current_length = self.get_compressed_page_size(0);

            for i in 1..page_count {
                let offset = self.get_offset(i);
                let length = self.get_compressed_page_size(i);

                if (current_length + current_length) as i64 == offset {
                    current_length += length;
                } else {
                    start_list.push(current_offset as usize);
                    length_list.push(current_length as usize);
                    current_offset = offset;
                    current_length = length
                }
            }
            start_list.push(current_offset as usize);
            length_list.push(current_length as usize);
        }
        (start_list, length_list)
    }
}
