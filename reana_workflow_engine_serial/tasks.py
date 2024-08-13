# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2018, 2019, 2020, 2021, 2022 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA Workflow Engine Serial tasks."""

from __future__ import absolute_import, print_function

import logging
import os

from reana_commons.config import REANA_LOG_FORMAT, REANA_LOG_LEVEL
from reana_commons.serial import serial_load
from reana_commons.workflow_engine import create_workflow_engine_command

from .config import CACHE_ENABLED, WORKFLOW_KERBEROS
from .utils import (
    build_job_spec,
    check_cache,
    copy_workspace_from_cache,
    copy_workspace_to_cache,
    get_targeted_workflow_steps,
    poll_job_status,
    publish_cache_copy,
    publish_job_submission,
    publish_job_success,
    publish_workflow_failure,
    publish_workflow_start,
)


def initialize(workflow_workspace, operational_options):
    """Initialize engine."""
    # configure the logger
    logging.basicConfig(level=REANA_LOG_LEVEL, format=REANA_LOG_FORMAT, filename="/opt/app/log.log")

    # set cache on or off
    if not operational_options:
        operational_options = {}
    if CACHE_ENABLED:
        if (
            "CACHE" not in operational_options
            or operational_options.get("CACHE", "").lower() != "off"
        ):
            cache_enabled = True
        else:
            cache_enabled = False
    else:
        cache_enabled = False

    return workflow_workspace, cache_enabled


def run(
    publisher,
    rjc_api_client,
    workflow_json,
    workflow_parameters,
    operational_options,
    workflow_uuid,
    workflow_workspace,
    cache_enabled,
):
    """Run a serial workflow."""
    operational_options = operational_options or {}
    publish_workflow_start(workflow_json.get("steps", []), workflow_uuid, publisher)

    expanded_workflow_json = serial_load(None, workflow_json, workflow_parameters)
    steps_to_run = get_targeted_workflow_steps(
        expanded_workflow_json,
        operational_options.get("TARGET", None),
        operational_options.get("FROM", None),
    )
    workflow_json["steps"] = expanded_workflow_json["steps"] = steps_to_run

    for step_number, step in enumerate(steps_to_run):
        status = run_step(
            rjc_api_client,
            step_number,
            step,
            workflow_workspace,
            cache_enabled,
            expanded_workflow_json,
            workflow_json,
            publisher,
            workflow_uuid,
        )
        if status != "finished":
            break


def run_step(
    rjc_api_client,
    step_number,
    step,
    workflow_workspace,
    cache_enabled,
    expanded_workflow_json,
    workflow_json,
    publisher,
    workflow_uuid,
):
    """Run a step of a serial workflow."""
    for command in step["commands"]:
        job_spec = build_job_spec(
            job_name=step.get("name", ""),
            image=step["environment"],
            compute_backend=step.get("compute_backend", ""),
            command=command,
            workflow_workspace=workflow_workspace,
            workflow_uuid=workflow_uuid,
            kerberos=step.get("kerberos", WORKFLOW_KERBEROS),
            unpacked_image=step.get("unpacked_image", False),
            kubernetes_uid=step.get("kubernetes_uid"),
            kubernetes_memory_limit=step.get("kubernetes_memory_limit"),
            kubernetes_job_timeout=step.get("kubernetes_job_timeout"),
            voms_proxy=step.get("voms_proxy", False),
            rucio=step.get("rucio", False),
            htcondor_max_runtime=step.get("htcondor_max_runtime", ""),
            htcondor_accounting_group=step.get("htcondor_accounting_group", ""),
            slurm_partition=step.get("slurm_partition"),
            slurm_time=step.get("slurm_time"),
        )
        job_spec_copy = dict(job_spec)
        job_spec_copy["cmd"] = command

        if cache_enabled:
            cached_info = check_cache(
                rjc_api_client, job_spec_copy, step, workflow_workspace
            )
            if (
                cached_info.get("result_path")
                and os.path.exists(cached_info.get("result_path"))
                and os.listdir(cached_info.get("result_path"))
            ):
                copy_workspace_from_cache(
                    cached_info["result_path"], workflow_workspace
                )
                publish_cache_copy(
                    cached_info["job_id"],
                    step,
                    expanded_workflow_json,
                    command,
                    publisher,
                    workflow_uuid,
                )
                continue
        response = rjc_api_client.submit(**job_spec)
        job_id = str(response["job_id"])
        publish_job_submission(
            step_number, command, workflow_json, job_id, publisher, workflow_uuid
        )

        job_status = poll_job_status(rjc_api_client, job_id)
        if job_status.status == "finished":
            cache_dir_path = None
            if cache_enabled:
                cache_dir_path = copy_workspace_to_cache(job_id, workflow_workspace)

            publish_job_success(
                job_id,
                job_spec,
                workflow_workspace,
                expanded_workflow_json,
                step,
                command,
                publisher,
                workflow_uuid,
                cache_dir_path=cache_dir_path,
            )
        else:
            publish_workflow_failure(job_id, workflow_uuid, publisher)
            return job_status.status
    return job_status.status


def run_serial_workflow_engine_adapter(
    publisher,
    rjc_api_client,
    workflow_uuid=None,
    workflow_json=None,
    workflow_workspace=None,
    workflow_parameters=None,
    operational_options=None,
    **kwargs
):
    """Run a serial workflow."""
    (
        workflow_workspace,
        cache_enabled,
    ) = initialize(workflow_workspace, operational_options)

    run(
        publisher,
        rjc_api_client,
        workflow_json,
        workflow_parameters,
        operational_options,
        workflow_uuid,
        workflow_workspace,
        cache_enabled,
    )


run_serial_workflow = create_workflow_engine_command(
    run_serial_workflow_engine_adapter, engine_type="serial"
)
