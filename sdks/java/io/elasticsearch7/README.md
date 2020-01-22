<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Elasticsearch7 IO module

This module is able to read, write, delete and update documents from/to Elasticsearch.
This is a complete rework of the current module.

# Why a new module ?

## Compatibility

The current module is compliant with Elasticsearch 2.x, 5.x and 6.x.
This seems to be a good point but so many things have been changed since Elasticsearch 2.x.

A fresh new module, only compliant with the last version of Elasticsearch, 
can easily benefit a lot from the last evolutions of Elasticsearch (Java High Level Http Client).

It is therefore far simpler than the current one.

## Error management

Currently, errors are caught and transformed into simple exceptions.
This is not always what is needed.
If we would like to do specific processing on these errors (send documents in error topics for instance), 
it is not possible with the current module.

## Philosophy

This module directly uses the Elasticsearch Java client classes as inputs and outputs.

# Writing to Elasticsearch

## Input

The module takes as input a `PCollection<T extends Writeable & DocWriteRequest<T>>`.

For instance:
- `PCollection<IndexRequest>`
- `PCollection<DeleteRequest>`
- `PCollection<UptdateRequest>`

This way you can configure any options you need directly in the `DocWriteRequest` objects.

For instance: 
- If you need to use [external versioning](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-versioning), you can.
- If you need to use an ingest pipeline, you can.
- If you need to configure an update document/script, you can.
- If you need to use upserts, you can.

Actually, you should be able to do everything you can do directly with Elasticsearch.

Furthermore, it should be easier to keep updating the module with future Elasticsearch evolutions.

## Output

Two outputs are available:
- Successful indexing output ;
- Failed indexing output.

They are available in a `WriteResult` object.

These two outputs are represented by `PCollection<BulkItemResponseContainer>` objects.

A `BulkItemResponseContainer` contains:
- the original index request ;
- the Elasticsearch response ;
- a batch id.

You can apply any process afterwards (reprocessing, alerting, ...).

## Example

You can find an example in the [unit tests](src/test/java/org/apache/beam/sdk/io/elasticsearch/utils/Elasticsearch7IOWritingTestRunner.java).

```java
    Elasticsearch7IO.Write.Builder<IndexRequest> writeBuilder = Elasticsearch7IO.write();

    List<String> input = generateDocuments(getDocumentCount(), getGenerator());

    PCollection<ESTestDocument> inputDocuments = getPipeline().apply("Generate input documents", Create.of(input));
    PCollection<IndexRequest> writeRequests = inputDocuments.apply("Map to WriteRequest", getWriteRequest()).setCoder(WriteableCoder.of());
    WriteResult<IndexRequest> writeResult = writeRequests.apply("Send to Elasticsearch", getWrite());

    PCollection<BulkItemResponseContainer<IndexRequest>> successfulIndexingResults = writeResult.getSuccessfulIndexing();
    PCollection<BulkItemResponseContainer<IndexRequest>> failedIndexingResults = writeResult.getFailedIndexing();

    // Now, do whatever you want with these outputs
```

# Reading from Elasticsearch

## Input

You can read documents from Elasticsearch with this module.
You can specify a `QueryBuilder` in order to filter the retrieved documents.
By default, it retrieves the whole document collection.

```java
    Read read = Elasticsearch7IO.Read.withQuery(QueryBuilders.idsQuery().addIds("0", "1", "2"))
                    .setConnectionConfiguration(connectionConfiguration)
                    .setBatchSize(1)
                    .build();
```

If the Elasticsearch index is sharded, multiple slices can be used during fetch.
That many bundles are created.
The maximum bundle count is equal to the index shard count.

## Output

The output is a `PCollection<SearchHit>`.

```java
    PCollection<SearchHit> documents = getPipeline()
            .apply("Read documents from Elasticsearch", read);
```

# Compatibility

This module is currently only compliant with Elasticsearch 7.x.