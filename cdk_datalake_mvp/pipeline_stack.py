#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: MIT-0

from constructs import Construct
from aws_cdk import (
    Stack,
    aws_codecommit as codecommit,
    pipelines as pipelines,
)
from cdk_datalake_mvp.pipeline_stage import DatalakePipelineStage


class DatalakePipelineStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Creates a CodeCommit repository called 'WorkshopRepo'
        repo = codecommit.Repository(
            self, 'datalake-mvp-cicd',
            repository_name= 'datalake-mvp-cicd'
        )
        
        # Pipeline code goes here
        pipeline = pipelines.CodePipeline(
            self,
            "Pipeline",
            self_mutation=True,
            synth=pipelines.ShellStep(
                "Synth",
                input=pipelines.CodePipelineSource.code_commit(repo, 'main'),
                commands=[
                    "npm install -g aws-cdk@latest",  # Installs the cdk cli on Codebuild
                    "pip install -r requirements.txt",  # Instructs Codebuild to install required packages
                    "cdk synth"
                    ]
            ),
        )
        deploy = DatalakePipelineStage(self, "Deploy")
        deploy_stage = pipeline.add_stage(deploy)