import * as cdk from 'aws-cdk-lib';
import { Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class Hw3Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // create an S3 bucket
    const bucket = new s3.Bucket(this, 'TestBucket', {
      bucketName: 's3-test-bucket-hw3',
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      blockPublicAccess: new s3.BlockPublicAccess({
        blockPublicAcls: false,
        blockPublicPolicy: false,
        ignorePublicAcls: false,
        restrictPublicBuckets: false
      }),
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_PREFERRED,
      publicReadAccess: true,

    });

    // create a DynamoDB table
    const table = new dynamodb.Table(this, 'SizeHistoryTable', {
      tableName: 's3-object-size-history',
      partitionKey: {
        name: 'bucket_name',
        type: dynamodb.AttributeType.STRING
      },
      sortKey: {
        name: 'timestamp',
        type: dynamodb.AttributeType.STRING
      },
      removalPolicy: RemovalPolicy.DESTROY,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST
    });

    table.addGlobalSecondaryIndex({
      indexName: 'bucket-size-index',
      partitionKey: { name: 'bucket_name', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'total_size', type: dynamodb.AttributeType.NUMBER }
    });

    // create a Lambda function to track the size of objects in the S3 bucket and store the history in DynamoDB
    // 1. define the Lambda function in Python
    const sizeTrackingLambda = new cdk.aws_lambda.Function(this, 'SizeTrackingLambda', {
      runtime: cdk.aws_lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: cdk.aws_lambda.Code.fromAsset('lambda/size_tracking'),
      timeout: cdk.Duration.seconds(30),
      environment: {
        BUCKET_NAME: bucket.bucketName,
        TABLE_NAME: table.tableName
      }
    });

    //2. grant the Lambda function permissions to read from the S3 bucket and write to the DynamoDB table
    bucket.grantRead(sizeTrackingLambda);
    table.grantWriteData(sizeTrackingLambda);

    // 3. add an event notification to the S3 bucket to trigger the Lambda function whenever an object is created or deleted
    bucket.addEventNotification(s3.EventType.OBJECT_CREATED, new cdk.aws_s3_notifications.LambdaDestination(sizeTrackingLambda));
    bucket.addEventNotification(s3.EventType.OBJECT_REMOVED, new cdk.aws_s3_notifications.LambdaDestination(sizeTrackingLambda));


    // generate the plot of the size history of the S3 bucket using a Lambda function

    // 1. deine matplotlib layer
    const matplotlibLayer = lambda.LayerVersion.fromLayerVersionArn(
      this,
      'MatplotlibLayer',
      'arn:aws:lambda:us-west-2:770693421928:layer:Klayers-p311-matplotlib:19'
    );
    // 2. define the Lambda function in Python
    const plottingLambda = new cdk.aws_lambda.Function(this, 'PlottingLambda', {
      runtime: cdk.aws_lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: cdk.aws_lambda.Code.fromAsset('lambda/plotting'),
      timeout: cdk.Duration.seconds(30),
      layers: [matplotlibLayer],
      environment: {
        BUCKET_NAME: bucket.bucketName,
        TABLE_NAME: table.tableName,
        KNOWN_BUCKETS: bucket.bucketName,
      }
    });

    // 3. grant the Lambda function permissions to read from the DynamoDB table and write to the S3 bucket
    table.grantReadData(plottingLambda);
    bucket.grantWrite(plottingLambda);

    // 4. add an API Gateway to trigger the plotting Lambda function
    const api = new apigateway.LambdaRestApi(this, 'PlottingApi', {
      handler: plottingLambda,
    });

    // define driver Lambda function to perform the sequence of operations on the S3 bucket and call the plotting API
    const driverLambda = new cdk.aws_lambda.Function(this, 'DriverLambda', {
      runtime: cdk.aws_lambda.Runtime.PYTHON_3_11,
      handler: 'handler.handler',
      code: cdk.aws_lambda.Code.fromAsset('lambda/driver'),
      timeout: cdk.Duration.seconds(60),
      environment: {
        BUCKET_NAME: bucket.bucketName,
        PLOTTING_API_URL: api.url
      }
    });

    // grant the driver Lambda function permissions to read and write to the S3 bucket
    bucket.grantReadWrite(driverLambda);
  }
}
