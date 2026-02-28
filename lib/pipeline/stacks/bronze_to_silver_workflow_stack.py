from dataclasses import dataclass

from aws_cdk import Aws, Duration, Stack, aws_glue
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk import aws_stepfunctions as sf
from aws_cdk import aws_stepfunctions_tasks as sf_tasks
from aws_cdk.aws_s3 import IBucket


@dataclass
class BronzeToSilverWorkflowStackProps:
    bronze_bucket: IBucket
    silver_bucket: IBucket
    athena_results_bucket: IBucket
    silver_db_name: str
    scripts_bucket: IBucket
    notification_email: str


class BronzeToSilverWorkflowStack(Stack):
    state_machine: sf.StateMachine

    def __init__(
        self, scope, construct_id, props: BronzeToSilverWorkflowStackProps, **kwargs
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            stack_name="UCEventsBronzeToSilverWfStack",
            **kwargs,
        )

        pipeline_failure_topic = sns.Topic(
            self,
            "WorkflowFailureTopic",
            display_name="UC Events Bronze to Silver Failures",
            topic_name=f"{construct_id}-failures",
        )

        pipeline_failure_topic.add_subscription(
            subscriptions.EmailSubscription(props.notification_email)
        )

        pipeline_success_topic = sns.Topic(
            self,
            "WorkflowSuccessTopic",
            display_name="Campus Events Bronze to Silver Success",
            topic_name=f"{construct_id}-success",
        )

        pipeline_success_topic.add_subscription(
            subscriptions.EmailSubscription(props.notification_email)
        )

        glue_role = iam.Role(
            scope=self,
            id="GlueJobServiceRole",
            role_name=f"{construct_id}-glue-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        props.scripts_bucket.grant_read(glue_role)
        props.bronze_bucket.grant_read_write(glue_role)
        props.silver_bucket.grant_read_write(glue_role)

        crawler_role = iam.Role(
            scope=self,
            id="CrawlerServiceRole",
            role_name=f"{construct_id}-crawler-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        props.silver_bucket.grant_read(crawler_role)

        glue_job_name = f"{construct_id}-data-job"
        glue.CfnJob(
            scope=self,
            id="BronzeToSilverJob",
            name=glue_job_name,
            role=glue_role.role_arn,
            glue_version="5.1",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=10,
            max_retries=0,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{props.scripts_bucket.bucket_name}/glue/bronze_to_silver.py",
            ),
            default_arguments={
                "--TempDir": f"s3://{props.bronze_bucket.bucket_name}/bronze_to_silver/",
                "--extra-py-files": f"s3://{props.scripts_bucket.bucket_name}/ce_types.py",
                "--continuous-log-logGroup": f"/aws-glue/jobs/{glue_job_name}",
                "--enable-spark-ui": "true",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--additional-python-modules": "feedparser,beautifulsoup4,pyarrow",
                "--SOURCE_BUCKET_NAME": props.bronze_bucket.bucket_name,
                "--TARGET_BUCKET_NAME": props.silver_bucket.bucket_name,
            },
        )

        glue.CfnCrawler(
            scope=self,
            id="SilverCrawler",
            name=f"{construct_id}-silver-crawler",
            role=crawler_role.role_arn,
            database_name=props.silver_db_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{props.silver_bucket.bucket_name}/ce_events/",
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="LOG",
            ),
        )

        list_files_fn = _lambda.Function(
            scope=self,
            id="ListFilesLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="list_brz_files_fn.handler",
            timeout=Duration.seconds(30),
            code=_lambda.Code.from_asset("lib/pipeline/functions"),
        )
        list_files_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[props.bronze_bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": ["new/*"]}},
            )
        )

        list_files_task = sf_tasks.LambdaInvoke(
            scope=self,
            id="ListUnprocessedFiles",
            lambda_function=list_files_fn,
            payload=sf.TaskInput.from_object(
                {
                    "BRONZE_BUCKET": props.bronze_bucket.bucket_name,
                }
            ),
        )

        glue_task = sf_tasks.GlueStartJobRun(
            scope=self,
            id="RunDataGlueJob",
            glue_job_name=glue_job_name,
            integration_pattern=sf.IntegrationPattern.RUN_JOB,
            timeout=Duration.minutes(10),
            arguments=sf.TaskInput.from_object(
                {"--UNPROCESSED_SOURCE_KEY": sf.JsonPath.string_at("$")}
            ),
        )

        notify_success = sf_tasks.SnsPublish(
            scope=self,
            id="NotifySuccess",
            topic=pipeline_success_topic,
            subject="✅ Campus Events Data Pipeline - Bronze to Silver Workflow Processing Complete",
            message=sf.TaskInput.from_json_path_at("$"),
        )

        process_files_map = sf.Map(
            self,
            "ProcessNewFilesMap",
            items_path=sf.JsonPath.string_at("$.Payload"),
            max_concurrency=1,
        )

        process_files_map.iterator(
            glue_task.add_catch(
                sf_tasks.SnsPublish(
                    scope=self,
                    id="NotifyGlueJobFailure",
                    topic=pipeline_failure_topic,
                    subject="🚨 Campus Events Data Pipeline - Bronze to Silver Workflow Glue Job Processing Failed",
                    message=sf.TaskInput.from_json_path_at("$"),
                ).next(
                    sf.Fail(
                        scope=self,
                        id="GlueJobFailed",
                        cause="Bronze to Silver transformation failed",
                        error="GlueJobError",
                    )
                ),
                errors=["States.ALL"],
                result_path="$",
            )
        )

        definition = (
            list_files_task.add_catch(
                sf_tasks.SnsPublish(
                    scope=self,
                    id="NotifyListFilesFailure",
                    topic=pipeline_failure_topic,
                    subject="🚨 Campus Events Data Pipeline - Failed to list new files",
                    message=sf.TaskInput.from_json_path_at("$.error"),
                ).next(
                    sf.Fail(
                        scope=self,
                        id="ListFilesFailed",
                        cause="Failed to list new files",
                        error="ListFilesError",
                    )
                ),
                errors=["States.ALL"],
                result_path="$.error",
            )
            .next(
                process_files_map.add_catch(
                    sf_tasks.SnsPublish(
                        scope=self,
                        id="NotifyMapProcessingFailure",
                        topic=pipeline_failure_topic,
                        subject="🚨 Campus Events Data Pipeline - Failed to process files",
                        message=sf.TaskInput.from_json_path_at("$.error"),
                    ).next(
                        sf.Fail(
                            scope=self,
                            id="MapProcessingFailure",
                            cause="Failed to process files",
                            error="MapError",
                        )
                    ),
                    errors=["States.ALL"],
                    result_path="$.error",
                )
            )
            .next(notify_success)
        )

        self.state_machine = sf.StateMachine(
            scope=self,
            id="BronzeToSilverStateMachine",
            state_machine_name=f"{construct_id}-state-machine",
            definition_body=sf.DefinitionBody.from_chainable(definition),
            timeout=Duration.minutes(35),
            tracing_enabled=True,
        )

        # Grant Step Functions permission to publish to SNS
        pipeline_failure_topic.grant_publish(self.state_machine)
        pipeline_success_topic.grant_publish(self.state_machine)

        # Grant Step Functions permission to read job results from S3
        props.silver_bucket.grant_read(self.state_machine)
