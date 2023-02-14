#!/usr/bin/env python3

#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: MIT-0

import aws_cdk as cdk

from cdk_datalake_mvp.cdk_datalake_mvp_stack import CdkDatalakeMvpStack
from cdk_datalake_mvp.pipeline_stack import DatalakePipelineStack


app = cdk.App()
#CdkDatalakeMvpStack(app, "cdk-datalake-mvp-test")
DatalakePipelineStack(app, "DatalakePipelineStack")

app.synth()
