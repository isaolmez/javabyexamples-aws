package com.javabyexamples.aws.dynamodb.streams;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.javabyexamples.aws.dynamodb.maintenance.ItemOperations;
import java.util.List;

public class StreamOperations {

    private final AmazonDynamoDBStreams dynamoDBStreams = AmazonDynamoDBStreamsClientBuilder.defaultClient();
    private final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

    public void enableStream(String tableName) {
        final UpdateTableRequest updateTableRequest = new UpdateTableRequest()
          .withTableName(tableName)
          .withStreamSpecification(
            new StreamSpecification().withStreamEnabled(true).withStreamViewType(StreamViewType.KEYS_ONLY));

        amazonDynamoDB.updateTable(updateTableRequest);
    }

    public void disableStream(String tableName) {
        final UpdateTableRequest updateTableRequest = new UpdateTableRequest()
          .withTableName(tableName)
          .withStreamSpecification(
            new StreamSpecification().withStreamEnabled(false));

        amazonDynamoDB.updateTable(updateTableRequest);
    }

    public List<Stream> listStreams(String tableName) {
        final ListStreamsRequest listStreamsRequest = new ListStreamsRequest().withTableName(tableName);
        final ListStreamsResult listStreamsResult = dynamoDBStreams.listStreams(listStreamsRequest);
        for (Stream stream : listStreamsResult.getStreams()) {
            System.out.printf("- %s:%s%n", stream.getStreamArn(), stream.getStreamLabel());
        }
        return listStreamsResult.getStreams();
    }

    public void readStream(String streamArn) {
        final DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamArn(streamArn);
        final DescribeStreamResult describeStreamResult = dynamoDBStreams.describeStream(describeStreamRequest);

        for (Shard shard : describeStreamResult.getStreamDescription().getShards()) {
            final GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
              .withStreamArn(streamArn)
              .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)
              .withShardId(shard.getShardId());
            final GetShardIteratorResult shardIteratorResult = dynamoDBStreams
              .getShardIterator(getShardIteratorRequest);
            final GetRecordsResult records = dynamoDBStreams
              .getRecords(new GetRecordsRequest().withShardIterator(shardIteratorResult.getShardIterator()));

            System.out.println(records.getRecords());
        }
    }

    public static void main(String[] args) {
        final String tableName = "javabyexamples";
        final StreamOperations streamOperations = new StreamOperations();
//        System.out.println("Enable stream.");
//        streamOperations.enableStream(tableName);

        System.out.println("List streams.");
        final List<Stream> streams = streamOperations.listStreams(tableName);

//        final ItemOperations itemOperations = new ItemOperations();
//        itemOperations.putItem(tableName, UUID.randomUUID().toString());
//        itemOperations.putItem(tableName, UUID.randomUUID().toString());
//        itemOperations.putItem(tableName, UUID.randomUUID().toString());

        System.out.println("Read stream.");
        streamOperations.readStream(streams.get(0).getStreamArn());

        System.out.println("Disable stream.");
        streamOperations.disableStream(tableName);
    }
}
