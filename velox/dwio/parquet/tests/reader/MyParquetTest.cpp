/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/tests/ParquetTestBase.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/parse/TypeResolver.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

#include <folly/init/Init.h>
#include <algorithm>

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

class MyParquetTest : public ParquetTestBase {
 public:
  void SetUp() override {
    dwio::common::LocalFileSink::registerFactory();
    rootPool_ = memory::memoryManager()->addRootPool("ParquetTests");
    leafPool_ = rootPool_->addLeafChild("ParquetTests");
    tempPath_ = exec::test::TempDirectoryPath::create();

    // Register type resolver with DuckDB SQL parser.
    parse::registerTypeResolver();
    // Register functions
    functions::prestosql::registerAllScalarFunctions();
  }

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const std::string& fileName,
      const RowTypePtr& rowType) {
    const std::string sample(getExampleFilePath(fileName));

    dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    auto reader = createReader(sample, readerOptions);

    RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
            rowType, rowType->names()));
    rowReaderOpts.setScanSpec(makeScanSpec(rowType));
    auto rowReader = reader->createRowReader(rowReaderOpts);
    return rowReader;
  }

  void assertReadWithExpected(
      const std::string& fileName,
      const RowTypePtr& rowType,
      const RowVectorPtr& expected) {
    auto rowReader = createRowReader(fileName, rowType);
    assertReadWithReaderAndExpected(rowType, *rowReader, expected, *pool_);
  }

  void assertReadWithFilters(
      const std::string& fileName,
      const RowTypePtr& fileSchema,
      FilterMap filters,
      const RowVectorPtr& expected) {
    const auto filePath(getExampleFilePath(fileName));
    dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    auto reader = createReader(filePath, readerOpts);
    assertReadWithReaderAndFilters(
        std::move(reader), fileName, fileSchema, std::move(filters), expected);
  }
};

TEST_F(MyParquetTest, parseSample) {
  // learn it
  // sample.parquet holds two columns (a: BIGINT, b: DOUBLE) and
  // 20 rows (10 rows per group). Group offsets are 153 and 614.
  // Data is in plain uncompressed format:
  //   a: [1..20]
  //   b: [1.0..20.0]
  const std::string sample(getExampleFilePath("sample.parquet"));

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReader(sample, readerOptions);
  EXPECT_EQ(reader->numberOfRows(), 20ULL);

  auto type = reader->typeWithId();
  EXPECT_EQ(type->size(), 2ULL);
  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::BIGINT);
  auto col1 = type->childAt(1);
  EXPECT_EQ(col1->type()->kind(), TypeKind::DOUBLE);
  EXPECT_EQ(type->childByName("a"), col0);
  EXPECT_EQ(type->childByName("b"), col1);

  auto rowReaderOpts = getReaderOpts(sampleSchema());
  auto scanSpec = makeScanSpec(sampleSchema());
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>(20, [](auto row) { return row + 1; }),
      makeFlatVector<double>(20, [](auto row) { return row + 1; }),
  });

  assertReadWithReaderAndExpected(
      sampleSchema(), *rowReader, expected, *leafPool_);
}

TEST_F(MyParquetTest, scanAndSort) {
  int argc = 1;
  char* args[] = {const_cast<char*>("scan_and_sort")};
  char** argv = args;

  // Velox Tasks/Operators are based on folly's async framework, so we need to
  // make sure we initialize it first.
  // 已经init了folly?
  // folly::Init init{&argc, &argv};

  // test base里已经initialize MemoryManager
  // Default memory allocator used throughout this example.
  // memory::MemoryManager::initialize({});
  auto pool = memory::memoryManager()->addLeafPool();

  // For this example, the input dataset will be comprised of a single BIGINT
  // column ("my_col"), containing 10 rows.
  auto inputRowType = ROW({{"my_col", BIGINT()}});
  const size_t vectorSize = 10;

  // Create a base flat vector and fill it with consecutive integers, then
  // shuffle them.
  auto vector = BaseVector::create(BIGINT(), vectorSize, pool.get());
  auto rawValues = vector->values()->asMutable<int64_t>();

  std::iota(rawValues, rawValues + vectorSize, 0); // 0, 1, 2, 3, ...
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(rawValues, rawValues + vectorSize, g);

  // Wrap the vector (column) in a RowVector.
  auto rowVector = std::make_shared<RowVector>(
      pool.get(), // pool where allocations will be made.
      inputRowType, // input row type (defined above).
      BufferPtr(nullptr), // no nulls on this example.
      vectorSize, // length of the vectors.
      std::vector<VectorPtr>{vector}); // the input vector data.

  // For fun, let's print the shuffled data to stdout.
  LOG(INFO) << "Input vector generated:";
  for (vector_size_t i = 0; i < rowVector->size(); ++i) {
    LOG(INFO) << rowVector->toString(i);
  }

  // In order to read and write data and files from storage, we need to use a
  // Connector. Let's instantiate and register a HiveConnector for this
  // example:

  // We need a connector id string to identify the connector.
  const std::string kHiveConnectorId = "test-hive";

  // Register the Hive Connector Factory.
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  // Create a new connector instance from the connector factory and register
  // it:
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()));
  connector::registerConnector(hiveConnector);

  // To be able to read local files, we need to register the local file
  // filesystem. We also need to register the dwrf reader factory as well as a
  // write protocol, in this case commit is not required:
  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();

  // Create a temporary dir to store the local file created. Note that this
  // directory is automatically removed when the `tempDir` object runs out of
  // scope.
  auto tempDir = exec::test::TempDirectoryPath::create();
  auto absTempDirPath = tempDir->getPath();

  // Once we finalize setting up the Hive connector, let's define our query
  // plan. We use the helper `PlanBuilder` class to generate the query plan
  // for this example, but this is usually done programatically based on the
  // application's IR. Considering that the plan executed in a local host is
  // usually part of a larger and often distributed query, this local portion is
  // called a "query fragment" and described by a "plan fragment".
  //
  // The first part of this example creates a plan fragment that writes data
  // to a file using TableWriter operator. It uses a special `Values` operator
  // that allows us to insert our RowVector straight into the pipeline.
  auto writerPlanFragment =
      exec::test::PlanBuilder()
          .values({rowVector})
          .tableWrite(absTempDirPath, dwio::common::FileFormat::DWRF)
          .planFragment();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  // Task is the top-level execution concept. A task needs a taskId (as a
  // string), the plan fragment to execute, a destination (only used for
  // shuffles), and a QueryCtx containing metadata and configs for a query.
  auto writeTask = exec::Task::create(
      "my_write_task",
      writerPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  // next() starts execution using the client thread. The loop pumps output
  // vectors out of the task (there are none in this query fragment).
  while (auto result = writeTask->next())
    ;

  // At this point, the first part of the example is done; there is now a
  // file encoded using dwrf at `filePath`. The next part of the example
  // will create a query plan that reads and sorts the contents of that file.
  //
  // The second query fragment is composed of a simple TableScan (which will
  // read the file we just generated), and subsequently sort it. After we create
  // the query fragment and Task structures, we add input data to the Task by
  // adding "splits" to it. Splits are added to the TableScan operator, so
  // during creation we need to capture the TableScan planNodeId in the
  // `scanNodeId` variable.
  core::PlanNodeId scanNodeId;
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(inputRowType)
                              .capturePlanNodeId(scanNodeId)
                              .orderBy({"my_col"}, /*isPartial=*/false)
                              .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  // Now that we have the query fragment and Task structure set up, we will
  // add data to it via `splits`.
  //
  // To pump data through a HiveConnector, we need to create a
  // HiveConnectorSplit for each file, using the same HiveConnector id defined
  // above, the local file path (the "file:" prefix specifies which FileSystem
  // to use; local, in this case), and the file format (DWRF/ORC).
  for (auto& filePath : fs::directory_iterator(absTempDirPath)) {
    auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
        kHiveConnectorId,
        "file:" + filePath.path().string(),
        dwio::common::FileFormat::DWRF);
    // Wrap it in a `Split` object and add to the task. We need to specify to
    // which operator we're adding the split (that's why we captured the
    // TableScan's id above). Here we could pump subsequent split/files into the
    // TableScan.
    readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  }

  // Signal that no more splits will be added. After this point, calling next()
  // on the task will start the plan execution using the current thread.
  readTask->noMoreSplits(scanNodeId);

  // Read output vectors and print them.
  while (auto result = readTask->next()) {
    LOG(INFO) << "Vector available after processing (scan + sort):";
    for (vector_size_t i = 0; i < result->size(); ++i) {
      LOG(INFO) << result->toString(i);
    }
  }
}

TEST_F(MyParquetTest, readSampleParquet) {
  auto pool = memory::memoryManager()->addLeafPool();

  auto inputRowType = ROW({{"a", INTEGER()}, {"b", DOUBLE()}});

  // In order to read and write data and files from storage, we need to use a
  // Connector. Let's instantiate and register a HiveConnector for this
  // example:
  // We need a connector id string to identify the connector.
  const std::string kHiveConnectorId = "test-hive";

  // Register the Hive Connector Factory.
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  // Create a new connector instance from the connector factory and register
  // it:
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()));
  connector::registerConnector(hiveConnector);

  // To be able to read local files, we need to register the local file
  // filesystem. We also need to register the dwrf reader factory as well as a
  // write protocol, in this case commit is not required:
  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();
  parquet::registerParquetReaderFactory();

  auto absTempDirPath = "/root/data/sample_parquet_dir";

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  // The second query fragment is composed of a simple TableScan (which will
  // read the file we just generated), and subsequently sort it. After we create
  // the query fragment and Task structures, we add input data to the Task by
  // adding "splits" to it. Splits are added to the TableScan operator, so
  // during creation we need to capture the TableScan planNodeId in the
  // `scanNodeId` variable.
  core::PlanNodeId scanNodeId;
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(inputRowType)
                              .capturePlanNodeId(scanNodeId)
//                              .orderBy({"a"}, /*isPartial=*/false)
                              .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      core::QueryCtx::create(executor.get()),
      exec::Task::ExecutionMode::kSerial);

  // Now that we have the query fragment and Task structure set up, we will
  // add data to it via `splits`.
  //
  // To pump data through a HiveConnector, we need to create a
  // HiveConnectorSplit for each file, using the same HiveConnector id defined
  // above, the local file path (the "file:" prefix specifies which FileSystem
  // to use; local, in this case), and the file format (DWRF/ORC).
  for (auto& filePath : fs::directory_iterator(absTempDirPath)) {
    auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
        kHiveConnectorId,
        "file:" + filePath.path().string(),
        dwio::common::FileFormat::PARQUET);
    // Wrap it in a `Split` object and add to the task. We need to specify to
    // which operator we're adding the split (that's why we captured the
    // TableScan's id above). Here we could pump subsequent split/files into the
    // TableScan.
    readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  }

  // Signal that no more splits will be added. After this point, calling next()
  // on the task will start the plan execution using the current thread.
  readTask->noMoreSplits(scanNodeId);

  // Read output vectors and print them.
  int c = 0;
  while (auto result = readTask->next()) {
    c++;
    LOG(INFO) << "Vector available after scan, vector index: " << c - 1
              << ", size: " << result->size();
    for (vector_size_t i = 0; i < result->size(); ++i) {
      LOG(INFO) << result->toString(i);
    }
  }
}

TEST_F(MyParquetTest, readStoreSalesParquet) {
  auto pool = memory::memoryManager()->addLeafPool();

  auto inputRowType = ROW({
      {"ss_sold_time_sk", INTEGER()},
      {"ss_item_sk", INTEGER()},
      {"ss_customer_sk", INTEGER()},
      {"ss_cdemo_sk", INTEGER()},
      {"ss_hdemo_sk", INTEGER()},
      {"ss_addr_sk", INTEGER()},
      {"ss_store_sk", INTEGER()},
      {"ss_promo_sk", INTEGER()},
      {"ss_ticket_number", INTEGER()},
      {"ss_quantity", INTEGER()},
      {"ss_wholesale_cost", DECIMAL(7, 2)},
      {"ss_list_price", DECIMAL(7, 2)},
      {"ss_sales_price", DECIMAL(7, 2)},
      {"ss_ext_discount_amt", DECIMAL(7, 2)},
      {"ss_ext_sales_price", DECIMAL(7, 2)},
      {"ss_ext_wholesale_cost", DECIMAL(7, 2)},
      {"ss_ext_list_price", DECIMAL(7, 2)},
      {"ss_ext_tax", DECIMAL(7, 2)},
      {"ss_coupon_amt", DECIMAL(7, 2)},
      {"ss_net_paid", DECIMAL(7, 2)},
      {"ss_net_paid_inc_tax", DECIMAL(7, 2)},
      {"ss_net_profit", DECIMAL(7, 2)},
      {"ss_sold_date_sk", INTEGER()}
  });

  // In order to read and write data and files from storage, we need to use a
  // Connector. Let's instantiate and register a HiveConnector for this
  // example:
  // We need a connector id string to identify the connector.
  const std::string kHiveConnectorId = "test-hive";

  // Register the Hive Connector Factory.
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  // Create a new connector instance from the connector factory and register
  // it:
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()));
  connector::registerConnector(hiveConnector);

  // To be able to read local files, we need to register the local file
  // filesystem. We also need to register the dwrf reader factory as well as a
  // write protocol, in this case commit is not required:
  filesystems::registerLocalFileSystem();
  dwio::common::registerFileSinks();
  parquet::registerParquetReaderFactory();

  auto absTempDirPath = "/root/data/store_sales_dir";

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  // The second query fragment is composed of a simple TableScan (which will
  // read the file we just generated), and subsequently sort it. After we create
  // the query fragment and Task structures, we add input data to the Task by
  // adding "splits" to it. Splits are added to the TableScan operator, so
  // during creation we need to capture the TableScan planNodeId in the
  // `scanNodeId` variable.
  core::PlanNodeId scanNodeId;
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(inputRowType)
                              .capturePlanNodeId(scanNodeId)
                              .filter("ss_sold_time_sk = 30946")
                              // .orderBy({"a"}, /*isPartial=*/false)
                              .planFragment();

  // Create the reader task.
  auto queryCtx = core::QueryCtx::create(executor.get());
  queryCtx->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kMaxOutputBatchRows, "1314" /*default 10000*/}});
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      queryCtx,
      exec::Task::ExecutionMode::kSerial);

  // Now that we have the query fragment and Task structure set up, we will
  // add data to it via `splits`.
  //
  // To pump data through a HiveConnector, we need to create a
  // HiveConnectorSplit for each file, using the same HiveConnector id defined
  // above, the local file path (the "file:" prefix specifies which FileSystem
  // to use; local, in this case), and the file format (PARQUET).
  for (auto& filePath : fs::directory_iterator(absTempDirPath)) {
    auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
        kHiveConnectorId,
        "file:" + filePath.path().string(),
        dwio::common::FileFormat::PARQUET);
    // Wrap it in a `Split` object and add to the task. We need to specify to
    // which operator we're adding the split (that's why we captured the
    // TableScan's id above). Here we could pump subsequent split/files into the
    // TableScan.
    readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  }

  // Signal that no more splits will be added. After this point, calling next()
  // on the task will start the plan execution using the current thread.
  readTask->noMoreSplits(scanNodeId);

  // Read output vectors and print them.
  int printBatchSize = 5;
  int c = 0;
  int printResultSize = 10;
  while (auto result = readTask->next()) {
    if (++c >= printBatchSize) {
      break;
    }
    LOG(INFO) << "Vector available after scan, vector index: " << c - 1
              << ", value size: " << result->size();
    for (vector_size_t i = 0; i < result->size(); ++i) {
      if (i >= printResultSize) {
        break;
      }
      // LOG(INFO) << result->toString(i);
      LOG(INFO) << result->loadedVector()->as<RowVector>()->toString(i);
    }
    LOG(INFO) << "\n";
  }
  LOG(INFO) << "Finished load!";
}
