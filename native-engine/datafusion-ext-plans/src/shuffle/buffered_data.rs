// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;

use arrow::record_batch::RecordBatch;
use auron_jni_bridge::{is_task_running, jni_call};
use bytesize::ByteSize;
use count_write::CountWrite;
use datafusion::{common::Result, physical_plan::metrics::Time};
use datafusion_ext_commons::{
    arrow::{
        array_size::BatchSize,
        selection::{BatchInterleaver, create_batch_interleaver},
    },
    compute_suggested_batch_size_for_output, df_execution_err,
    io::ipc_compression::IpcCompressionWriter,
};
use jni::objects::GlobalRef;
#[cfg(test)]
use parking_lot::Mutex;

use crate::{
    common::timer_helper::TimerHelper,
    shuffle::{
        Partitioning, evaluate_hashes, evaluate_partition_ids, evaluate_range_partition_ids,
        evaluate_robin_partition_ids, rss::RssWriter,
    },
};

pub struct BufferedData {
    partition_id: usize,
    partitioning: Partitioning,
    staging_batches: Vec<RecordBatch>,
    staging_num_rows: usize,
    staging_mem_used: usize,
    sorted_batches: Vec<RecordBatch>,
    sorted_offsets: Vec<Vec<u32>>,
    num_rows: usize,
    sorted_mem_used: usize,
    output_io_time: Time,
}

impl BufferedData {
    pub fn new(partitioning: Partitioning, partition_id: usize, output_io_time: Time) -> Self {
        Self {
            partition_id,
            partitioning,
            staging_batches: vec![],
            staging_num_rows: 0,
            staging_mem_used: 0,
            sorted_batches: vec![],
            sorted_offsets: vec![],
            num_rows: 0,
            sorted_mem_used: 0,
            output_io_time,
        }
    }

    pub fn drain(&mut self) -> Self {
        std::mem::replace(
            self,
            Self::new(
                self.partitioning.clone(),
                self.partition_id,
                self.output_io_time.clone(),
            ),
        )
    }

    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // first add to staging, mem used is doubled for later sorting
        self.num_rows += batch.num_rows();
        self.staging_num_rows += batch.num_rows();
        self.staging_mem_used += batch.get_batch_mem_size() * 2;
        self.staging_batches.push(batch);

        let suggested_batch_size =
            compute_suggested_batch_size_for_output(self.staging_mem_used, self.staging_num_rows);
        if self.staging_num_rows >= suggested_batch_size {
            self.flush_staging()?;
        }
        Ok(())
    }

    fn flush_staging(&mut self) -> Result<()> {
        let sorted_num_rows = self.num_rows - self.staging_num_rows;
        let staging_batches = std::mem::take(&mut self.staging_batches);
        let (offsets, sorted_batch) = sort_batches_by_partition_id(
            staging_batches,
            &self.partitioning,
            sorted_num_rows,
            self.partition_id,
        )?;
        self.staging_num_rows = 0;
        self.staging_mem_used = 0;

        self.sorted_mem_used += sorted_batch.get_batch_mem_size() + offsets.len() * 4;
        self.sorted_batches.push(sorted_batch);
        self.sorted_offsets.push(offsets);
        Ok(())
    }

    // write buffered data to spill/target file, returns uncompressed size and
    // offsets to each partition
    pub fn write<W: Write>(mut self, mut w: W) -> Result<Vec<u64>> {
        if self.num_rows == 0 {
            return Ok(vec![0; self.partitioning.partition_count() + 1]);
        }

        let mem_used = ByteSize(self.mem_used() as u64);
        log::info!("draining all buffered data, total_mem={mem_used}");

        if !self.staging_batches.is_empty() {
            self.flush_staging()?;
        }

        let output_io_time = self.output_io_time.clone();
        let num_partitions = self.partitioning.partition_count();
        let mut writer = IpcCompressionWriter::new(CountWrite::from(&mut w));
        let mut offsets = vec![];
        let mut iter = self.into_sorted_batches()?;

        while let Some((partition_id, batch_iter)) = iter.next_partition_chunk() {
            if !is_task_running() {
                df_execution_err!("task completed/killed")?;
            }

            offsets.resize(partition_id + 1, writer.inner().count());
            for batch in batch_iter {
                output_io_time
                    .with_timer(|| writer.write_batch(batch.num_rows(), batch.columns()))?;
            }
            output_io_time.with_timer(|| writer.finish_current_buf())?;
        }
        offsets.resize(num_partitions + 1, writer.inner().count());

        let compressed_size = ByteSize(offsets.last().cloned().unwrap_or_default());
        log::info!("all buffered data drained, compressed_size={compressed_size}");
        Ok(offsets)
    }

    // write buffered data to rss, returns uncompressed size
    pub fn write_rss(mut self, rss_partition_writer: GlobalRef) -> Result<()> {
        if self.num_rows == 0 {
            return Ok(());
        }

        let mem_used = ByteSize(self.mem_used() as u64);
        log::info!("draining all buffered data to rss, total_mem={mem_used}");

        if !self.staging_batches.is_empty() {
            self.flush_staging()?;
        }

        let output_io_time = self.output_io_time.clone();
        let mut iter = self.into_sorted_batches()?;
        let mut writer = IpcCompressionWriter::new(RssWriter::new(rss_partition_writer.clone(), 0));
        let mut total_pushed: u64 = 0;

        while let Some((partition_id, batch_iter)) = iter.next_partition_chunk() {
            if !is_task_running() {
                df_execution_err!("task completed/killed")?;
            }

            // write all batches with this part id
            writer.set_output(RssWriter::new(rss_partition_writer.clone(), partition_id));
            for batch in batch_iter {
                output_io_time
                    .with_timer(|| writer.write_batch(batch.num_rows(), batch.columns()))?;
            }
            output_io_time.with_timer(|| writer.finish_current_buf())?;
            total_pushed += writer.inner().total_written();
        }

        output_io_time.with_timer(
            || jni_call!(AuronRssPartitionWriterBase(rss_partition_writer.as_obj()).flush() -> ()),
        )?;
        log::info!(
            "all buffered data drained to rss, pushed_compressed={}, uncompressed_mem={}",
            ByteSize(total_pushed),
            mem_used
        );
        Ok(())
    }

    fn into_sorted_batches(self) -> Result<PartitionedBatchesIterator<'static>> {
        let num_rows = self.num_rows;
        let sub_batch_size = compute_suggested_batch_size_for_output(self.mem_used(), num_rows);
        let num_partitions = self.partitioning.partition_count();
        PartitionedBatchesIterator::try_new(
            self.sorted_batches,
            self.sorted_offsets,
            sub_batch_size,
            num_partitions,
        )
    }

    pub fn mem_used(&self) -> usize {
        self.sorted_mem_used + self.staging_mem_used
    }

    pub fn is_empty(&self) -> bool {
        self.sorted_batches.is_empty() && self.staging_batches.is_empty()
    }
}

/// Iterates buffered rows partition by partition. Within a partition, rows are
/// emitted in input order: sorted batches in the order they were flushed, and
/// rows within each batch in the order they were added. All sorted batches are
/// in memory, so each partition's slices are taken directly from the per-batch
/// offsets.
struct PartitionedBatchesIterator<'a> {
    batch_interleaver: BatchInterleaver,
    batch_offsets: Vec<Vec<u32>>,
    num_partitions: usize,
    next_partition_id: usize,
    batch_size: usize,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> PartitionedBatchesIterator<'a> {
    pub fn try_new(
        batches: Vec<RecordBatch>,
        batch_offsets: Vec<Vec<u32>>,
        sub_batch_size: usize,
        num_partitions: usize,
    ) -> Result<Self> {
        Ok(Self {
            batch_interleaver: create_batch_interleaver(&batches, true)?,
            batch_offsets,
            num_partitions,
            next_partition_id: 0,
            batch_size: sub_batch_size,
            _phantom: Default::default(),
        })
    }

    /// Returns the next non-empty partition and an iterator over its rows, in
    /// input order, as batches of at most `batch_size` rows.
    pub fn next_partition_chunk(
        &mut self,
    ) -> Option<(usize, impl Iterator<Item = RecordBatch> + 'a)> {
        // safety: bypass lifetime checker
        let batches_iter =
            unsafe { std::mem::transmute::<_, &mut PartitionedBatchesIterator<'a>>(self) };

        while batches_iter.next_partition_id < batches_iter.num_partitions {
            let partition_id = batches_iter.next_partition_id;
            batches_iter.next_partition_id += 1;

            let ranges = batches_iter
                .batch_offsets
                .iter()
                .enumerate()
                .filter_map(|(batch_idx, offsets)| {
                    let range = offsets[partition_id] as usize..offsets[partition_id + 1] as usize;
                    (!range.is_empty()).then_some((batch_idx, range))
                })
                .collect::<Vec<_>>();
            if ranges.is_empty() {
                continue;
            }

            let batch_size = batches_iter.batch_size;
            let mut rows = ranges
                .into_iter()
                .flat_map(|(batch_idx, range)| range.map(move |row_idx| (batch_idx, row_idx)));
            let batch_interleaver = &mut batches_iter.batch_interleaver;
            let batch_iter = std::iter::from_fn(move || {
                let indices = rows.by_ref().take(batch_size).collect::<Vec<_>>();
                if indices.is_empty() {
                    return None;
                }
                Some(batch_interleaver(&indices).expect("error interleaving batches"))
            });
            return Some((partition_id, batch_iter));
        }
        None
    }
}

fn sort_batches_by_partition_id(
    batches: Vec<RecordBatch>,
    partitioning: &Partitioning,
    current_num_rows: usize,
    partition_id: usize,
) -> Result<(Vec<u32>, RecordBatch)> {
    let num_partitions = partitioning.partition_count();
    let mut round_robin_start_rows =
        (partition_id * 1000193 + current_num_rows) % partitioning.partition_count();

    // compute partition indices
    let partition_indices = batches
        .iter()
        .enumerate()
        .flat_map(|(batch_idx, batch)| {
            let part_ids = match partitioning {
                Partitioning::HashPartitioning(..) => {
                    // compute partition indices
                    let hashes = evaluate_hashes(partitioning, &batch)
                        .expect(&format!("error evaluating hashes with {partitioning}"));
                    evaluate_partition_ids(hashes, partitioning.partition_count())
                }
                Partitioning::RoundRobinPartitioning(..) => {
                    let part_ids =
                        evaluate_robin_partition_ids(partitioning, &batch, round_robin_start_rows);
                    round_robin_start_rows += batch.num_rows();
                    round_robin_start_rows %= partitioning.partition_count();
                    part_ids
                }
                Partitioning::RangePartitioning(sort_expr, _, bounds) => {
                    evaluate_range_partition_ids(&batch, sort_expr, bounds)
                        .expect("failed to evaluate range partition ids")
                }
                _ => unreachable!("unsupported partitioning: {:?}", partitioning),
            };
            part_ids
                .into_iter()
                .enumerate()
                .map(move |(row_idx, part_id)| (part_id, batch_idx as u32, row_idx as u32))
        })
        .collect::<Vec<_>>();

    // group rows by partition id with a stable counting sort, so rows of the same
    // partition keep their input order. An in-place (unstable) sort scrambles them,
    // which hurts compression of sorted or clustered data (apache/auron#1782).
    let mut part_counts = vec![0usize; num_partitions];
    for &(part_id, ..) in &partition_indices {
        part_counts[part_id as usize] += 1;
    }

    // compute partitions
    let mut partition_offsets = Vec::with_capacity(num_partitions + 1);
    let mut offset = 0;
    for &part_count in &part_counts {
        partition_offsets.push(offset);
        offset += part_count as u32;
    }
    partition_offsets.push(offset);

    // place rows at their partition's next slot, in input order
    let mut next_slots = partition_offsets[..num_partitions]
        .iter()
        .map(|&o| o as usize)
        .collect::<Vec<_>>();
    let mut sorted_indices = vec![(0usize, 0usize); partition_indices.len()];
    for (part_id, batch_idx, row_idx) in partition_indices {
        let slot = &mut next_slots[part_id as usize];
        sorted_indices[*slot] = (batch_idx as usize, row_idx as usize);
        *slot += 1;
    }

    // get sorted batch
    let batches_interleaver = create_batch_interleaver(&batches, true)?;
    let sorted_batch = batches_interleaver(&sorted_indices)?;
    return Ok((partition_offsets, sorted_batch));
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
        row::{RowConverter, Rows, SortField},
    };
    use arrow_schema::SortOptions;
    use datafusion::{
        assert_batches_eq,
        common::Result,
        physical_expr::{PhysicalSortExpr, expressions::Column},
    };

    use super::*;

    fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Result<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )?;
        Ok(batch)
    }

    #[test]
    fn test_add_batch_flushes_by_row_count() -> Result<()> {
        let values = (0..1000).collect::<Vec<_>>();
        let record_batch = build_table_i32(("a", &values), ("b", &values), ("c", &values))?;
        let batch_mem_used = record_batch.get_batch_mem_size() * 2;
        let suggested_batch_size =
            compute_suggested_batch_size_for_output(batch_mem_used, record_batch.num_rows());

        // Ensure this fixture distinguishes byte count from row count.
        assert!(batch_mem_used > suggested_batch_size);
        assert!(record_batch.num_rows() < suggested_batch_size);

        let num_batches = suggested_batch_size.div_ceil(record_batch.num_rows());
        let mut buffered_data =
            BufferedData::new(Partitioning::RoundRobinPartitioning(4), 0, Time::new());

        for _ in 1..num_batches {
            buffered_data.add_batch(record_batch.clone())?;
        }
        assert!(buffered_data.sorted_batches.is_empty());

        buffered_data.add_batch(record_batch)?;
        assert_eq!(buffered_data.sorted_batches.len(), 1);
        assert_eq!(
            buffered_data.sorted_batches[0].num_rows(),
            num_batches * values.len()
        );
        assert!(buffered_data.staging_batches.is_empty());
        Ok(())
    }

    #[test]
    fn test_partitions_keep_input_order() -> Result<()> {
        // Within each output partition, rows must be written in input order, across
        // sorted batches and within each batch. Keeping input order makes sorted or
        // clustered keys compress much better (apache/auron#1782).
        let rows_per_input = 1000;
        let probe = (0..rows_per_input).collect::<Vec<i32>>();
        let probe = build_table_i32(("a", &probe), ("b", &probe), ("c", &probe))?;
        let batches_per_flush = compute_suggested_batch_size_for_output(
            probe.get_batch_mem_size() * 2,
            probe.num_rows(),
        )
        .div_ceil(rows_per_input as usize);
        let num_sorted_batches = 4;
        let total_rows = batches_per_flush * num_sorted_batches * rows_per_input as usize;

        let partitionings = [
            Partitioning::RoundRobinPartitioning(4),
            // hash partitioning spreads rows unevenly, so batches have different
            // sets of empty partitions
            Partitioning::HashPartitioning(vec![Arc::new(Column::new("a", 0))], 7),
        ];
        for partitioning in partitionings {
            let mut buffered_data = BufferedData::new(partitioning.clone(), 0, Time::new());
            for i in 0..batches_per_flush * num_sorted_batches {
                let start = i as i32 * rows_per_input;
                let values = (start..start + rows_per_input).collect::<Vec<_>>();
                buffered_data.add_batch(build_table_i32(
                    ("a", &values),
                    ("b", &values),
                    ("c", &values),
                )?)?;
            }
            assert_eq!(buffered_data.sorted_batches.len(), num_sorted_batches);

            let mut iter = buffered_data.into_sorted_batches()?;
            let mut num_rows = 0;
            while let Some((partition_id, batch_iter)) = iter.next_partition_chunk() {
                let mut prev: Option<i32> = None;
                for batch in batch_iter {
                    let col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("Int32Array");
                    for &v in col.values() {
                        if let Some(p) = prev {
                            assert!(
                                v > p,
                                "{partitioning}: partition {partition_id} wrote row {v} after row {p}"
                            );
                        }
                        prev = Some(v);
                        num_rows += 1;
                    }
                }
            }
            assert_eq!(num_rows, total_rows, "{partitioning}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_round_robin() -> Result<()> {
        let record_batch = build_table_i32(
            ("a", &vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        )?;

        let round_robin_partitioning = Partitioning::RoundRobinPartitioning(4);
        let (_parts, sorted_batch) =
            sort_batches_by_partition_id(vec![record_batch], &round_robin_partitioning, 3, 0)?;

        // rows of each partition keep their input order
        let expected = vec![
            "+----+---+---+",
            "| a  | b | c |",
            "+----+---+---+",
            "| 18 | 1 | 6 |",
            "| 14 | 5 | 0 |",
            "| 10 | 9 | 4 |",
            "| 17 | 2 | 7 |",
            "| 13 | 6 | 1 |",
            "| 16 | 3 | 8 |",
            "| 12 | 7 | 2 |",
            "| 19 | 0 | 5 |",
            "| 15 | 4 | 9 |",
            "| 11 | 8 | 3 |",
            "+----+---+---+",
        ];
        assert_batches_eq!(expected, &vec![sorted_batch]);
        Ok(())
    }

    #[tokio::test]
    async fn test_range_partition() -> Result<()> {
        let record_batch = build_table_i32(
            ("a", &vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        )?;
        let sort_exprs = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("a", 0)),
            options: SortOptions::default(),
        }];
        let bound1 = Arc::new(Int32Array::from_iter_values([11, 14, 17])) as ArrayRef;
        let bounds = vec![bound1];

        let sort_row_converter = Arc::new(Mutex::new(RowConverter::new(
            sort_exprs
                .iter()
                .map(|expr: &PhysicalSortExpr| {
                    Ok(SortField::new_with_options(
                        expr.expr.data_type(&record_batch.schema())?,
                        expr.options,
                    ))
                })
                .collect::<Result<Vec<SortField>>>()?,
        )?));

        let rows: Rows = sort_row_converter.lock().convert_columns(&bounds)?;
        let partition_num = rows.num_rows() + 1;

        let range_repartitioning =
            Partitioning::RangePartitioning(sort_exprs, partition_num, Arc::from(rows));
        let (_parts, sorted_batch) =
            sort_batches_by_partition_id(vec![record_batch], &range_repartitioning, 0, 0)?;

        let expected = vec![
            "+----+---+---+",
            "| a  | b | c |",
            "+----+---+---+",
            "| 11 | 8 | 3 |",
            "| 10 | 9 | 4 |",
            "| 14 | 5 | 0 |",
            "| 13 | 6 | 1 |",
            "| 12 | 7 | 2 |",
            "| 17 | 2 | 7 |",
            "| 16 | 3 | 8 |",
            "| 15 | 4 | 9 |",
            "| 19 | 0 | 5 |",
            "| 18 | 1 | 6 |",
            "+----+---+---+",
        ];
        assert_batches_eq!(expected, &vec![sorted_batch]);
        Ok(())
    }

    #[tokio::test]
    async fn test_range_partition_2() -> Result<()> {
        let record_batch = build_table_i32(
            ("a", &vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        )?;
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];
        let bound1 = Arc::new(Int32Array::from_iter_values([11, 14, 17])) as ArrayRef;
        let bound2 = Arc::new(Int32Array::from_iter_values([1, 3, 5])) as ArrayRef;

        let bounds = vec![bound1, bound2];

        let sort_row_converter = Arc::new(Mutex::new(RowConverter::new(
            sort_exprs
                .iter()
                .map(|expr: &PhysicalSortExpr| {
                    Ok(SortField::new_with_options(
                        expr.expr.data_type(&record_batch.schema())?,
                        expr.options,
                    ))
                })
                .collect::<Result<Vec<SortField>>>()?,
        )?));

        let rows: Rows = sort_row_converter.lock().convert_columns(&bounds)?;
        let partition_num = rows.num_rows() + 1;

        let range_repartitioning =
            Partitioning::RangePartitioning(sort_exprs, partition_num, Arc::from(rows));
        let (_parts, sorted_batch) =
            sort_batches_by_partition_id(vec![record_batch], &range_repartitioning, 0, 0)?;

        let expected = vec![
            "+----+---+---+",
            "| a  | b | c |",
            "+----+---+---+",
            "| 10 | 9 | 4 |",
            "| 13 | 6 | 1 |",
            "| 12 | 7 | 2 |",
            "| 11 | 8 | 3 |",
            "| 17 | 2 | 7 |",
            "| 16 | 3 | 8 |",
            "| 15 | 4 | 9 |",
            "| 14 | 5 | 0 |",
            "| 19 | 0 | 5 |",
            "| 18 | 1 | 6 |",
            "+----+---+---+",
        ];
        assert_batches_eq!(expected, &vec![sorted_batch]);
        Ok(())
    }
}
