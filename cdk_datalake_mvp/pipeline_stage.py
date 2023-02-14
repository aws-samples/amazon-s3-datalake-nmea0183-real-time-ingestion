#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: MIT-0

from constructs import Construct
from aws_cdk import (
    Stage
)
from .cdk_datalake_mvp_stack import CdkDatalakeMvpStack

class DatalakePipelineStage(Stage):

    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        service = CdkDatalakeMvpStack(self, 'Datalake')