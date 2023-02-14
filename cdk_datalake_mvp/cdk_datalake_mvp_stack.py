#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: MIT-0

import resource
import os
import json
import boto3
from datetime import datetime
from constructs import Construct
from aws_cdk import (
    Size,
    Duration,
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_lambda as _lambda,
    aws_sns_subscriptions as subs,
    aws_apigateway as apigateway,
    aws_glue as glue,
    aws_kinesisfirehose as firehose,
    aws_location as location,
    aws_athena as athena
)
from aws_solutions_constructs.aws_kinesis_firehose_s3 import KinesisFirehoseToS3
from constructs import Construct


class CdkDatalakeMvpStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        def get_account_id():
            # Create an STS client
            sts_client = boto3.client('sts')
    
            # Call the STS client to retrieve the account ID
            response = sts_client.get_caller_identity()
            account_id = response['Account']
            return account_id


        account_id = get_account_id()
        
        # Store the value of the variable in the environment
        #def store_variable(variable_name, value):
            #os.environ[variable_name] = value

        # Retrieve the value of the variable from the environment
        #def retrieve_variable(variable_name):
            #return os.environ[variable_name]

        
        #variable_name = 'account_id'
        #value = account_id
        #store_variable(variable_name, value)
        #stored_value = retrieve_variable(variable_name)
        
        kinesis_delivery_stream_name = "firehose_s3_mvp"
        api_gateway_name = "apigateway_to_firehose"
        bucket_name = "mvp-cdk-bucket-" + account_id
        athena_bucket_name = "dl-athenaresults-" + account_id
        glue_database_name = "glue_database_mvp"

        bucket = s3.Bucket(self, bucket_name,block_public_access=s3.BlockPublicAccess.BLOCK_ALL, encryption=s3.BucketEncryption.S3_MANAGED, bucket_name=bucket_name)

        athenaresultsbucket = s3.Bucket(self,athena_bucket_name,block_public_access=s3.BlockPublicAccess.BLOCK_ALL, encryption=s3.BucketEncryption.S3_MANAGED, bucket_name=athena_bucket_name)

        cfn_place_index = location.CfnPlaceIndex(self, "MyCfnPlaceIndex", data_source="Here", index_name="DeviceLoc_" + account_id)

        glue_role = iam.Role(self, "GlueRole",
            assumed_by = iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies = [iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')]
        )

        firehose_extended_config_role = iam.Role(self, "FirehoseExtendedConfigRole",
            assumed_by = iam.ServicePrincipal('firehose.amazonaws.com'),
            managed_policies = [iam.ManagedPolicy.from_aws_managed_policy_name('AWSLambda_FullAccess'),iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')]
        )

        firehose_extended_config_policy = iam.Policy(
            self, "FirehoseExtendedConfigPolicy",
            policy_name = "FirehosePolicy",
            statements = [
                iam.PolicyStatement(
                    effect = iam.Effect.ALLOW,
                    actions = [ 
                        "s3:*"

                    ],
                    resources = [
                        bucket.bucket_arn,
                        bucket.bucket_arn+"/*"
                    ]
                ),              
            ]               
        )
        firehose_extended_config_policy.attach_to_role(firehose_extended_config_role)   
        


        glue_extended_policy = iam.Policy(
            self, "ExtendedPolicyGlue",
            policy_name = "DataLakeAccess_Policy",
            statements = [
                iam.PolicyStatement(
                    effect = iam.Effect.ALLOW,
                    actions = [
                        "s3:PutObject",
                        "s3:GetObject"

                    ],
                    resources = [
                        bucket.bucket_arn,
                        bucket.bucket_arn+"/*"
                    ]
                )
            ]               
        )

        glue_extended_policy.attach_to_role(glue_role)

        my_lambda = _lambda.Function(
            self, 'HelloHandler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.from_asset("cdk_datalake_mvp/lambda"),
            handler="hello.lambda_handler",
            function_name="heartbeat-decode",
            timeout=Duration.seconds(900),
            environment= {
                "dLake_name": bucket_name,
                "index_name": "DeviceLoc_" + account_id
                }
        )

        #
        my_lambda.add_alias("Live")

        lambda_custom = iam.Policy(
            self, "LambdaCustomPolicy",
            policy_name = "LambdaCustomPolicy",
            statements = [
                iam.PolicyStatement(
                    effect = iam.Effect.ALLOW,
                    actions = [ 
                        "s3:PutObject",
                        "geo:SearchPlaceIndexForPosition"
                    ],
                    resources = [
                        "arn:aws:s3:::"+bucket_name+"/*",
                        "arn:aws:geo:us-east-1:"+account_id+":place-index/DeviceLoc_" + account_id
                    ]
                ),              
            ]               
        )

        my_lambda.role.attach_inline_policy(lambda_custom)

        extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
        bucket_arn=f"arn:aws:s3:::{bucket_name}",
        role_arn=firehose_extended_config_role.role_arn,

        # the properties below are optional
        buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
            interval_in_seconds=60,
            size_in_m_bs=64
        ),
        cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
            enabled=True,
            log_group_name="/aws/kinesisfirehose/imodal_message_delivery",
            log_stream_name="DestinationDelivery"
        ),
        compression_format="UNCOMPRESSED",
        data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
            enabled=True,
            input_format_configuration=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                deserializer=firehose.CfnDeliveryStream.DeserializerProperty(
                    # hive_json_ser_de=kinesisfirehose.CfnDeliveryStream.HiveJsonSerDeProperty(
                    #     timestamp_formats=["timestampFormats"]
                    # ),
                    open_x_json_ser_de=firehose.CfnDeliveryStream.OpenXJsonSerDeProperty(
                        case_insensitive=False,
                        column_to_json_key_mappings={
                            "column_to_json_key_mappings_key": "columnToJsonKeyMappings"
                        },
                        convert_dots_in_json_keys_to_underscores=False
                    )
                )
            ),
            output_format_configuration=firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
                serializer=firehose.CfnDeliveryStream.SerializerProperty(
                    parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty(
                    )
                )
            ),
            schema_configuration=firehose.CfnDeliveryStream.SchemaConfigurationProperty(
                catalog_id=account_id,
                database_name=glue_database_name,
                region="us-east-1",
                role_arn=firehose_extended_config_role.role_arn,
                table_name="stage",
                version_id="LATEST"
            )
        ),
         dynamic_partitioning_configuration=firehose.CfnDeliveryStream.DynamicPartitioningConfigurationProperty(
             enabled=True,
             retry_options=firehose.CfnDeliveryStream.RetryOptionsProperty(
                 duration_in_seconds=300
             ),
             
         ),
        error_output_prefix="error/",
        prefix="stage/!{partitionKeyFromQuery:year}/!{partitionKeyFromQuery:month}/!{partitionKeyFromQuery:day}/",
        processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
            enabled=True,
            processors=[firehose.CfnDeliveryStream.ProcessorProperty(
                type="Lambda",

                # the properties below are optional
                parameters=[firehose.CfnDeliveryStream.ProcessorParameterProperty(
                    parameter_name="LambdaArn",
                    parameter_value=my_lambda.function_arn
                )
                ]
            ),
            firehose.CfnDeliveryStream.ProcessorProperty(
                type="MetadataExtraction",
                    
                # the properties below are optional
                parameters=[firehose.CfnDeliveryStream.ProcessorParameterProperty(
                    parameter_name="MetadataExtractionQuery",
                    parameter_value='{year:.date_time| strftime("%Y"), month:.date_time| strftime("%m"), day:.date_time| strftime("%d")}'    #'year:.date_time|strftime("%Y")'
                ),
                firehose.CfnDeliveryStream.ProcessorParameterProperty(
                    parameter_name="JsonParsingEngine",
                    parameter_value="JQ-1.6"
                )])
        ]
    ),
    s3_backup_mode="Disabled")

        kinesis_firehose = KinesisFirehoseToS3(self, kinesis_delivery_stream_name, 
        kinesis_firehose_props=firehose.CfnDeliveryStreamProps(
            delivery_stream_name=kinesis_delivery_stream_name,
            extended_s3_destination_configuration=extended_s3_destination_configuration,
            delivery_stream_type="DirectPut",
                

        ),
        existing_bucket_obj=bucket

        )

        kinesis_delivery_stream_arn = Stack.format_arn(self, 
            service='firehose',
            resource='deliverystream',
            resource_name=kinesis_delivery_stream_name
        )
                

        # API Role and Policy to write to Firehose
        api_fh_role = iam.Role(self, "apigw-to-fh-role",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com")
        )
        
        api_fh_role.add_to_policy(iam.PolicyStatement(
            resources=[kinesis_firehose.kinesis_firehose.attr_arn],
            actions=["firehose:PutRecord", "firehose:PutRecordBatch"]
        ))

        # API Request Template mapping to transform data when writing to Firehose
        api_to_fh_template_section = dict(
            DeliveryStreamName="$input.params('stream-name')",
            Record= dict(Data="$util.base64Encode($input.json('$'))")
        )
            
        
        api_to_fh_request_mapping = {"application/json": f"{json.dumps(api_to_fh_template_section,indent = 2)}"}


        # API Gateway Definition
        api = apigateway.RestApi(self, api_gateway_name,
            rest_api_name=api_gateway_name,
            #api_key_source_type=imodal_api_key,
            deploy_options=apigateway.StageOptions(
                                     logging_level=apigateway.MethodLoggingLevel.INFO,
                                     data_trace_enabled=True,
                                     stage_name="dev",
                                     tracing_enabled=True
                                 ),
            cloud_watch_role=True
        )

        fh_backend = api.root.add_resource("{stream-name}")
        
        fh_backend_integration = apigateway.AwsIntegration(
            service="firehose",
            path="",
            region=os.environ["CDK_DEFAULT_REGION"],
            action='PutRecord',
            integration_http_method='POST',
            options=dict(
                credentials_role=api_fh_role, 
                request_templates=api_to_fh_request_mapping,
                integration_responses=[apigateway.IntegrationResponse(
                    status_code='200',
#                    response_templates={"application/json": '{"status": "OK"}'}
                )]
            )
        )
        fh_backend.add_method(
            "POST", 
            fh_backend_integration, 
            method_responses=[{"statusCode":"200"}]
        )



        GlueCrawler = glue.CfnCrawler(
            self,
            "GlueCrawler",
            name="mvp_crawler_stage",
            role=glue_role.role_arn,
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression="cron(0 4 * * ? *)"),  
            targets={
                "s3Targets": [{"path": f"s3://{bucket_name}/stage/"}]
            },
            database_name=glue_database_name,
            schema_change_policy={
                "update_behavior": "LOG",
                "delete_behavior": "LOG"
            },
            configuration="{\"Version\":1.0,\"Grouping\":{\"TableGroupingPolicy\":\"CombineCompatibleSchemas\"}}"
        )
        
        gluedatabase = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=account_id,
            database_input={
                "name": glue_database_name,
                "description": "imodaldb"
            },            
        )

        gluetable = glue.CfnTable(
            self,
            "GlueTable",
            catalog_id=account_id,
            database_name=glue_database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="stage",
                description="stage",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "crawlerSchemaDeserializerVersion": "1.0",
                    "crawlerSchemaSerializerVersion": "1.0",
                    "updated_by_crawler": "imodaldb_stage-test",
                    "classification": "parquet",
                    "compressionType": "none",
                    "delimiter": ",",
                    "has_encrypted_data": "false",
                    "averageRecordSize": "1152",
                    "typeOfData": "file",
                    "sizeKey": "933014",
                    "record_count": "321",

                },
                
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(
                                name='packetid',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='devicetype',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='deviceid',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='userapplicationid',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='organizationid',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='len',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='status',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='hiverxtime',
                                type='string'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='longitude',
                                type='double'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='latitude',
                                type='double'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='addressnumber',
                                type='string'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='street',
                                type='string'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='municipality',
                                type='string'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='region',
                                type='string'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='subregion',
                                type='string'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='postalcode',
                                type='string'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='country',
                                type='string'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='timezone_name',
                                type='string'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='timezone_offset',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='solar_panel_current',
                                type='double'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='battery_current',
                                type='double'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='solar_panel_voltage',
                                type='double'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='battery_voltage',
                                type='double'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='date_time',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='no_messages_sent_since_last_power_cycle',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='altitude',
                                type='double'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='speed',
                                type='double'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='course',
                                type='double'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='last_rssi_value',
                                type='int'
                        ),
                        glue.CfnTable.ColumnProperty(
                                name='modem_current',
                                type='double'
                        ),
                    ],
                    location="s3://"+bucket_name+"/stage/",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=
                    {
                        'serializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'parameters': { 'serialization.format': 1 },
                    }
                

            )
                

            
        )

        )
        gluetable.add_depends_on(gluedatabase)


