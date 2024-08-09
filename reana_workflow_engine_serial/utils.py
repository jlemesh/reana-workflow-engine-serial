# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2019, 2020, 2021, 2022 CERN.
#
# REANA is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""REANA-Workflow-Engine-Serial utilities."""

import logging
import os
from distutils.dir_util import copy_tree
from time import sleep

from reana_commons.utils import build_caching_info_message, build_progress_message

from .config import JOB_STATUS_POLLING_INTERVAL, MOUNT_CVMFS


def build_job_spec(
    job_name,
    image,
    compute_backend,
    command,
    workflow_workspace,
    workflow_uuid,
    kerberos,
    unpacked_image,
    kubernetes_uid,
    kubernetes_memory_limit,
    kubernetes_job_timeout,
    voms_proxy,
    rucio,
    htcondor_max_runtime,
    htcondor_accounting_group,
    slurm_partition,
    slurm_time,
):
    """Build job specification to passed to RJC."""
    job_spec = {
        "image": image,
        "compute_backend": compute_backend,
        "cmd": "cd {0} && {1}".format(workflow_workspace, command),
        "prettified_cmd": command,
        "workflow_workspace": workflow_workspace,
        "job_name": job_name,
        "cvmfs_mounts": MOUNT_CVMFS,
        "workflow_uuid": workflow_uuid,
        "kerberos": kerberos,
        "unpacked_img": unpacked_image,
        "kubernetes_uid": kubernetes_uid,
        "kubernetes_memory_limit": kubernetes_memory_limit,
        "kubernetes_job_timeout": kubernetes_job_timeout,
        "voms_proxy": voms_proxy,
        "rucio": rucio,
        "htcondor_max_runtime": htcondor_max_runtime,
        "htcondor_accounting_group": htcondor_accounting_group,
        "slurm_partition": slurm_partition,
        "slurm_time": slurm_time,
    }
    return job_spec


def check_cache(rjc_api_client, job_spec_copy, step, workflow_workspace):
    """Check if job exists in cache."""
    http_response = rjc_api_client.check_if_cached(
        job_spec_copy, step, workflow_workspace
    )
    result = http_response.json()
    if result["cached"]:
        return result
    return {}


def copy_workspace_from_cache(result_path, workflow_workspace):
    """Restore workspace contents from cache."""
    os.system(
        "cp -R {source} {dest}".format(
            source=os.path.join(result_path, "*"), dest=workflow_workspace
        )
    )


def copy_workspace_to_cache(job_id, workflow_workspace):
    """Copy workspace contents to cache."""
    logging.info("Caching result to ../archive/{}".format(job_id))
    logging.info("workflow_workspace: {}".format(workflow_workspace))

    # Create the cache directory if it doesn't exist
    cache_dir_path = os.path.abspath(
        os.path.join(workflow_workspace, os.pardir, "archive", job_id)
    )
    logging.info("cache_dir_path: {}".format(cache_dir_path))
    os.makedirs(cache_dir_path)

    # Copy workspace contents to cache directory
    copy_tree(workflow_workspace, cache_dir_path)
    return cache_dir_path


def publish_cache_copy(
    job_id, step, expanded_workflow_json, command, publisher, workflow_uuid
):
    """Publish to MQ the cache hit."""
    logging.info("Copied from cache")
    if step == expanded_workflow_json["steps"][-1] and command == step["commands"][-1]:
        workflow_status = 2
    else:
        workflow_status = 1
    finished_jobs = {"total": 1, "job_ids": [job_id]}
    publisher.publish_workflow_status(
        workflow_uuid,
        workflow_status,
        message={
            "progress": build_progress_message(
                finished=finished_jobs, cached=finished_jobs
            )
        },
    )


def publish_job_submission(
    step_number, command, workflow_json, job_id, publisher, workflow_uuid
):
    """Publish to MQ the job submission."""
    logging.info(
        "Publishing step:{0}, cmd: {1},"
        " total steps {2} to MQ".format(
            step_number, command, len(workflow_json["steps"])
        )
    )
    running_jobs = {"total": 1, "job_ids": [job_id]}

    publisher.publish_workflow_status(
        workflow_uuid,
        status=1,
        message={"progress": build_progress_message(running=running_jobs)},
    )


def poll_job_status(rjc_api_client, job_id):
    """Poll for job status."""
    job_status = rjc_api_client.check_status(job_id)

    while job_status.status not in ["finished", "failed", "stopped"]:
        job_status = rjc_api_client.check_status(job_id)
        sleep(JOB_STATUS_POLLING_INTERVAL)

    return job_status


def publish_job_success(
    job_id,
    job_spec,
    workflow_workspace,
    expanded_workflow_json,
    step,
    command,
    publisher,
    workflow_uuid,
    cache_dir_path=None,
):
    """Publish to MQ the job success."""
    finished_jobs = {"total": 1, "job_ids": [job_id]}
    if step == expanded_workflow_json["steps"][-1] and command == step["commands"][-1]:
        workflow_status = 2
    else:
        workflow_status = 1

    message = {}
    message["progress"] = build_progress_message(finished=finished_jobs)
    if cache_dir_path:
        message["caching_info"] = build_caching_info_message(
            job_spec, job_id, workflow_workspace, step, cache_dir_path
        )
    message["pod_name"] = os.getenv("WORKFLOW_POD_NAME")
    publisher.publish_workflow_status(workflow_uuid, workflow_status, message=message)


def publish_workflow_start(workflow_steps, workflow_uuid, publisher) -> None:
    """Publish to MQ the start of the workflow."""
    total_commands = 0
    for step in workflow_steps:
        total_commands += len(step["commands"])
    total_jobs = {"total": total_commands, "job_ids": []}
    publisher.publish_workflow_status(
        workflow_uuid, 1, message={"progress": build_progress_message(total=total_jobs), "pod_name": os.getenv("WORKFLOW_POD_NAME")}
    )


def publish_workflow_failure(job_id, workflow_uuid, publisher):
    """Publish to MQ the workflow failure."""
    failed_jobs = {"total": 1, "job_ids": [job_id]} if job_id else None

    publisher.publish_workflow_status(
        workflow_uuid,
        3,
        message={"progress": build_progress_message(failed=failed_jobs)},
    )


def get_targeted_workflow_steps(workflow_json, target_step=None, from_step=None):
    """Build the workflow steps until the given target step.

    :param workflow_json: Dictionary representing the serial workflow spec.
    :type dict:
    :param target_step: Step until which the workflow will be run identified
        by name.
    :param from_step: Step from which the workflow will be run identified
        by name.
    :type str:
    :returns: A list of the steps which should be run.
    :rtype: dict
    """
    from_step_idx = 0
    target_step_idx = len(workflow_json["steps"])
    if from_step:
        from_step_idx = next(
            (
                i
                for (i, step) in enumerate(workflow_json["steps"])
                if step["name"].lower() == from_step.lower()
            ),
            0,
        )
    if target_step:
        target_step_idx = next(
            (
                i + 1
                for (i, step) in enumerate(workflow_json["steps"])
                if step["name"].lower() == target_step.lower()
            ),
            target_step_idx,
        )
    if from_step_idx <= target_step_idx:
        return workflow_json["steps"][from_step_idx:target_step_idx]
    else:
        logging.error(
            "From step has to be the same as target or before target step. "
            "Executing full workflow."
        )
    return workflow_json["steps"]
