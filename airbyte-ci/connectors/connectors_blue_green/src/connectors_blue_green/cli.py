# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

from __future__ import annotations

import contextlib
import datetime
import os
import subprocess
import sys
import typing
from pathlib import Path

import click
import requests
import yaml
from connectors_blue_green import collectors, errors, logger, proxy, runners

if typing.TYPE_CHECKING:
    from typing import Tuple

TARGET_VERSION_VENV_PATH = "/tmp/next_version"


def get_metadata() -> dict:
    with open(os.environ["PATH_TO_METADATA"], "r") as metadata_file:
        return yaml.safe_load(metadata_file)


def get_current_version(metadata: dict):
    return metadata["data"]["dockerImageTag"]


def get_pipy_package_name(metadata: dict):
    pypi = metadata["data"].get("remoteRegistries").get("pypi")
    if not pypi:
        return None
    if not pypi["enabled"]:
        return None
    return pypi["packageName"]


def find_latest_version(package_name: str, current_version: str) -> str:
    response = requests.get(f"https://pypi.org/pypi/{package_name}/json")
    version_release_dates = [
        (k, datetime.datetime.strptime(v[0]["upload_time"], "%Y-%m-%dT%H:%M:%S")) for k, v in response.json()["releases"].items()
    ]
    sorted_versions = sorted(version_release_dates, key=lambda x: x[1], reverse=True)
    highest_version = sorted_versions[0][0]
    if highest_version == current_version:
        raise errors.NoTargetVersionError(f"No next version found for {package_name}")
    return highest_version


@contextlib.contextmanager
def virtualenv(venv_path: str) -> None:
    """A context manager to activate and deactivate a virtual environment."""

    # Save the current environment variables to restore them later
    old_path = os.environ.get("PATH")
    old_pythonpath = os.environ.get("PYTHONPATH")
    old_virtual_env = os.environ.get("VIRTUAL_ENV")

    # Activate the virtual environment
    activate_script = os.path.join(venv_path, "bin", "activate_this.py")
    exec(open(activate_script).read(), {"__file__": activate_script})

    try:
        yield
    finally:
        # Deactivate the virtual environment
        os.environ["PATH"] = old_path if old_path else ""
        if old_pythonpath is not None:
            os.environ["PYTHONPATH"] = old_pythonpath
        if old_virtual_env is not None:
            os.environ["VIRTUAL_ENV"] = old_virtual_env


def install_target_version(package_name: str, target_version: str) -> str:
    Path(TARGET_VERSION_VENV_PATH).mkdir(exist_ok=True)
    os.chdir(TARGET_VERSION_VENV_PATH)
    create_venv_result = subprocess.run(["uv", "--quiet", "venv"], capture_output=True, text=True)
    venv_path = f"{TARGET_VERSION_VENV_PATH}/.venv"
    with virtualenv(venv_path):
        install_result = subprocess.run(["uv", "--quiet", "pip", "install", f"{package_name}=={target_version}", "pip_system_certs"])
        if create_venv_result.returncode != 0 or install_result.returncode != 0:
            raise errors.InstallTargetVersionError(f"Could not install {package_name}=={target_version}")
    # TODO find a better way to get the path of the installed package
    return f"{venv_path}/bin/{package_name.replace('airbyte-', '')}"


def get_artifacts_directory_path(artifacts_root_directory: Path, package_name: str, package_version: str, entrypoint_command: str) -> Path:
    return artifacts_root_directory / package_name / package_version / entrypoint_command


def get_target_and_control_artifact_directories(
    artifacts_root_directory: Path, package_name: str, control_version: str, target_version: str, entrypoint_command: str
) -> Tuple[Path, Path]:
    return (
        get_artifacts_directory_path(artifacts_root_directory, package_name, control_version, entrypoint_command),
        get_artifacts_directory_path(artifacts_root_directory, package_name, target_version, entrypoint_command),
    )


@click.group
def connectors_blue_green() -> None:
    pass


@connectors_blue_green.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.option(
    "--artifacts-root-directory",
    envvar="BLUE_GREEN_DIR",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, writable=True, readable=True, path_type=Path),
)
@click.pass_context
def entrypoint(ctx: click.Context, artifacts_root_directory: Path):
    entrypoint_args = ctx.args
    entrypoint_command = entrypoint_args[0]
    metadata = get_metadata()
    package_name = get_pipy_package_name(metadata)
    control_version = get_current_version(metadata)
    target_version = find_latest_version(package_name, control_version)
    target_version_bin_path = install_target_version(package_name, target_version)
    logger.info(f"Installed target version: {target_version}")
    # TODO pass control_version_command as a CLI option?
    control_version_command = ["python", "/airbyte/integration_code/main.py"] + entrypoint_args
    target_version_command = [target_version_bin_path] + entrypoint_args

    control_artifacts_directory, target_artifacts_directory = get_target_and_control_artifact_directories(
        artifacts_root_directory, package_name, control_version, target_version, entrypoint_command
    )

    control_artifact_generator = collectors.ArtifactGenerator(control_artifacts_directory, entrypoint_args)
    target_artifact_generator = collectors.ArtifactGenerator(target_artifacts_directory, entrypoint_args)

    with proxy.MitmProxy(proxy_port=8080, har_dump_path=control_artifact_generator.har_dump_path) as control_proxy:
        logger.info("Running command on control version")
        control_exit_code = runners.run_command_and_stream_output(
            control_version_command, control_artifact_generator.process_line, tee=True
        )
    control_artifact_generator.save_artifacts()

    with proxy.MitmProxy(
        proxy_port=8081, har_dump_path=target_artifact_generator.har_dump_path, replay_session_path=control_proxy.session_path
    ):
        logger.info("Running command on target version")
        target_exit_code = runners.run_command_and_stream_output(target_version_command, target_artifact_generator.process_line, tee=False)
    target_artifact_generator.save_artifacts()
    sys.exit(control_exit_code)
