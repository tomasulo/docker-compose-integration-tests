package com.tomasulo.sample.infrastructure;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.tomasulo.sample.domain.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class UserRepository {

    private static final Logger logger = LoggerFactory.getLogger(UserRepository.class);

    private final DynamoDBMapper mapper;
    private final AmazonDynamoDB dynamoDB;

    public UserRepository(String dynamoEndpoint, String awsAccessKey, String awsSecretKey, String awsRegion) {
        this.dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        dynamoEndpoint, awsRegion))
                .withCredentials(new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(awsAccessKey, awsSecretKey)))
                .build();
        this.mapper = new DynamoDBMapper(dynamoDB);

        initializeTable();
    }

    private void initializeTable() {
        CreateTableRequest req = mapper.generateCreateTableRequest(User.class)
                .withProvisionedThroughput(new ProvisionedThroughput(10L, 5L));

        // create table if necessary
        TableUtils.createTableIfNotExists(dynamoDB, req);

        try {
            TableUtils.waitUntilActive(dynamoDB, User.TABLE_NAME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void upsert(User user) {
        logger.info("Upserting user: {}", user);
        mapper.save(user);
    }

    public User getById(String id) {
        User user = mapper.load(User.class, id);
        logger.debug("Retrieved user: {}", user);
        return user;
    }

    public void delete(User user) {
        logger.info("Deleting user: {}", user);
        mapper.delete(user);
    }

    public List<User> getAll() {
        PaginatedScanList<User> scan = mapper.scan(User.class, new DynamoDBScanExpression());
        List<User> users = scan.stream()
                .collect(Collectors.toList());
        logger.debug("Found {} users", users.size());
        return users;
    }
}
