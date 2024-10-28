package com.javabyexamples.aws.dynamodb.indexes;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class LocalSecondaryIndexOperations {

    private final AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

    public void createIndex(String tableName, String indexName) {
        final LocalSecondaryIndex localSecondaryIndex = new LocalSecondaryIndex().withIndexName(indexName)
          .withKeySchema(
            new KeySchemaElement("user", KeyType.HASH),
            new KeySchemaElement("post_date", KeyType.RANGE))
          .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY));

        final CreateTableRequest createTableRequest = new CreateTableRequest()
          .withTableName(tableName)
          .withKeySchema(new KeySchemaElement("user", KeyType.HASH),
            new KeySchemaElement("post_name", KeyType.RANGE))
          .withAttributeDefinitions(
            new AttributeDefinition("user", ScalarAttributeType.S),
            new AttributeDefinition("post_name", ScalarAttributeType.S),
            new AttributeDefinition("post_date", ScalarAttributeType.S))
          .withProvisionedThroughput(new ProvisionedThroughput(5L, 1L))
          .withLocalSecondaryIndexes(localSecondaryIndex);

        amazonDynamoDB.createTable(createTableRequest);
    }

    public static void main(String[] args) {
        final String tableName = "localSecondaryTable1";
        final String indexName = "localSecondary1";
        final LocalSecondaryIndexOperations indexOperations = new LocalSecondaryIndexOperations();
        indexOperations.createIndex(tableName, indexName);
    }
}
