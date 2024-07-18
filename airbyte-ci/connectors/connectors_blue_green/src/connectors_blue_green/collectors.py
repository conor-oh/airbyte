# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

# from airbyte_protocol.models import AirbyteStateMessage  # type: ignore
# from airbyte_protocol.models import AirbyteStreamStatusTraceMessage  # type: ignore
# from airbyte_protocol.models import ConfiguredAirbyteCatalog  # type: ignore
# from airbyte_protocol.models import TraceType  # type: ignore
from airbyte_protocol.models import AirbyteCatalog  # type: ignore
from airbyte_protocol.models import AirbyteMessage  # type: ignore
from airbyte_protocol.models import Type as AirbyteMessageType
from connectors_blue_green.file_backend import FileBackend
from connectors_blue_green.utils import hash_dict_values, sanitize_stream_name, sort_dict_keys
from genson import SchemaBuilder  # type: ignore
from pydantic import ValidationError

if TYPE_CHECKING:
    from pathlib import Path
    from typing import List


class ArtifactGenerator:
    def __init__(self, artifact_directory: Path, entrypoint_args: List[str]) -> None:
        self.artifact_directory = artifact_directory
        self.artifact_directory.mkdir(exist_ok=True, parents=True)
        self.har_dump_path = str(self.artifact_directory / "http_traffic.har")
        self.entrypoint_args = entrypoint_args
        self.stream_builders: dict[str, SchemaBuilder] = {}
        self.message_type_counts = {}
        self.messages_to_persist = []

    def process_line(self, line) -> None:
        try:
            message = AirbyteMessage.parse_raw(line)
            self.message_type_counts.setdefault(message.type.value, 0)
            self.message_type_counts[message.type.value] += 1

            if message.type is AirbyteMessageType.RECORD:
                self._process_record(message)
            else:
                self.messages_to_persist.append(message)

        except ValidationError as e:
            pass

    def get_option_value_path(self, option: str):
        for i, arg in enumerate(self.entrypoint_args):
            if arg == option and Path(self.entrypoint_args[i + 1]).exists():
                return Path(self.entrypoint_args[i + 1])

    @property
    def config_path(self) -> Path | None:
        return self.get_option_value_path("--config")

    @property
    def catalog_path(self) -> Path | None:
        return self.get_option_value_path("--catalog")

    @property
    def state_path(self) -> Path | None:
        return self.get_option_value_path("--state")

    def _process_record(self, record):
        self._update_schema(record)
        # TODO pks mgmt

    def _update_schema(self, record) -> None:
        stream = record.record.stream
        if stream not in self.stream_builders:
            stream_schema_builder = SchemaBuilder()
            stream_schema_builder.add_schema({"type": "object", "properties": {}})
            self.stream_builders[stream] = stream_schema_builder
        self.stream_builders[stream].add_object(record.record.data)

    def _get_stream_schemas(self) -> dict:
        return {stream: sort_dict_keys(self.stream_builders[stream].to_schema()) for stream in self.stream_builders}

    def _save_messages(self):
        file_backend = FileBackend(self.artifact_directory / "messages")
        file_backend.write(self.messages_to_persist)

    def _save_stream_schemas(self) -> None:
        stream_schemas_dir = self.artifact_directory / "stream_schemas"
        stream_schemas = self._get_stream_schemas()
        if not stream_schemas:
            return None
        stream_schemas_dir.mkdir(exist_ok=True)
        for stream_name, stream_schema in stream_schemas.items():
            (stream_schemas_dir / f"{sanitize_stream_name(stream_name)}.json").write_text(json.dumps(stream_schema, sort_keys=True))

    def _save_entrypoint_args(self) -> None:
        (self.artifact_directory / "entrypoint_args.txt").write_text(" ".join(self.entrypoint_args))

    def _save_config(self) -> None:
        if self.config_path:
            config = json.loads(self.config_path.read_text())
            hashed_config = hash_dict_values(config)
            (self.artifact_directory / "config.json").write_text(json.dumps(hashed_config))

    def _save_catalog(self) -> None:
        if self.catalog_path:
            (self.artifact_directory / "catalog.json").write_text(self.catalog_path.read_text())

    def _save_state(self) -> None:
        if self.state_path:
            (self.artifact_directory / "state.json").write_text(self.state_path.read_text())

    def _save_message_type_count(self) -> None:
        (self.artifact_directory / "message_type_counts.json").write_text(json.dumps(self.message_type_counts))

    def save_artifacts(self):
        self._save_entrypoint_args()
        self._save_messages()
        self._save_catalog()
        self._save_config()
        self._save_state()
        self._save_stream_schemas()
        self._save_message_type_count()
