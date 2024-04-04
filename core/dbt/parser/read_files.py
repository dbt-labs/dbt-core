import os
import pathspec  # type: ignore
import pathlib
from dataclasses import dataclass, field
from dbt_common.clients.system import load_file_contents
from dbt.contracts.files import (
    FilePath,
    ParseFileType,
    SourceFile,
    FileHash,
    AnySourceFile,
    SchemaSourceFile,
    FixtureSourceFile,
)
from dbt.config import Project
from dbt_common.dataclass_schema import dbtClassMixin
from dbt.parser.schemas import yaml_from_file, schema_file_keys
from dbt.exceptions import ParsingError
from dbt.parser.search import filesystem_search
from typing import Optional, Dict, List, Mapping, MutableMapping
from dbt.events.types import InputFileDiffError
from dbt_common.events.functions import fire_event
from typing import Protocol


@dataclass
class InputFile(dbtClassMixin):
    path: str
    content: str
    modification_time: float = 0.0


@dataclass
class FileDiff(dbtClassMixin):
    deleted: List[str]
    # Note: it would be possible to not distinguish between
    # added and changed files, but we would lose some error handling.
    changed: List[InputFile]
    added: List[InputFile]


# This loads the files contents and creates the SourceFile object
def load_source_file(
    path: FilePath,
    parse_file_type: ParseFileType,
    project_name: str,
    saved_files,
) -> Optional[AnySourceFile]:

    if parse_file_type == ParseFileType.Schema:
        sf_cls = SchemaSourceFile
    elif parse_file_type == ParseFileType.Fixture:
        sf_cls = FixtureSourceFile  # type:ignore[assignment]
    else:
        sf_cls = SourceFile  # type:ignore[assignment]

    source_file = sf_cls(
        path=path,
        checksum=FileHash.empty(),
        parse_file_type=parse_file_type,
        project_name=project_name,
    )

    skip_loading_schema_file = False
    if (
        parse_file_type == ParseFileType.Schema
        and saved_files
        and source_file.file_id in saved_files
    ):
        old_source_file = saved_files[source_file.file_id]
        if (
            source_file.path.modification_time != 0.0
            and old_source_file.path.modification_time == source_file.path.modification_time
        ):
            source_file.checksum = old_source_file.checksum
            source_file.dfy = old_source_file.dfy
            skip_loading_schema_file = True

    if not skip_loading_schema_file:
        # We strip the file_contents before generating the checksum because we want
        # the checksum to match the stored file contents
        file_contents = load_file_contents(path.absolute_path, strip=True)
        source_file.contents = file_contents
        source_file.checksum = FileHash.from_contents(file_contents)

    if parse_file_type == ParseFileType.Schema and source_file.contents:
        dfy = yaml_from_file(source_file)
        if dfy:
            validate_yaml(source_file.path.original_file_path, dfy)
            source_file.dfy = dfy
    return source_file


# Do some minimal validation of the yaml in a schema file.
# Check version, that key values are lists and that each element in
# the lists has a 'name' key
def validate_yaml(file_path, dct):
    for key in schema_file_keys:
        if key in dct:
            if not isinstance(dct[key], list):
                msg = (
                    f"The schema file at {file_path} is "
                    f"invalid because the value of '{key}' is not a list"
                )
                raise ParsingError(msg)
            for element in dct[key]:
                if not isinstance(element, dict):
                    msg = (
                        f"The schema file at {file_path} is "
                        f"invalid because a list element for '{key}' is not a dictionary"
                    )
                    raise ParsingError(msg)
                if "name" not in element:
                    msg = (
                        f"The schema file at {file_path} is "
                        f"invalid because a list element for '{key}' does not have a "
                        "name attribute."
                    )
                    raise ParsingError(msg)


# Special processing for big seed files
def load_seed_source_file(match: FilePath, project_name) -> SourceFile:
    if match.seed_too_large():
        # We don't want to calculate a hash of this file. Use the path.
        source_file = SourceFile.big_seed(match)
    else:
        file_contents = load_file_contents(match.absolute_path, strip=True)
        checksum = FileHash.from_contents(file_contents)
        source_file = SourceFile(path=match, checksum=checksum)
        source_file.contents = ""
    source_file.parse_file_type = ParseFileType.Seed
    source_file.project_name = project_name
    return source_file


# Use the FilesystemSearcher to get a bunch of FilePaths, then turn
# them into a bunch of FileSource objects
def get_source_files(project, paths, extension, parse_file_type, saved_files, ignore_spec):
    # file path list
    fp_list = filesystem_search(project, paths, extension, ignore_spec)
    # file block list
    fb_list = []
    for fp in fp_list:
        if parse_file_type == ParseFileType.Seed:
            fb_list.append(load_seed_source_file(fp, project.project_name))
        # singular tests live in /tests but only generic tests live
        # in /tests/generic and fixtures in /tests/fixture so we want to skip those
        else:
            if parse_file_type == ParseFileType.SingularTest:
                path = pathlib.Path(fp.relative_path)
                if path.parts[0] in ["generic", "fixtures"]:
                    continue
            file = load_source_file(fp, parse_file_type, project.project_name, saved_files)
            # only append the list if it has contents. added to fix #3568
            if file:
                fb_list.append(file)
    return fb_list


def read_files_for_parser(project, files, parse_ft, file_type_info, saved_files, ignore_spec):
    dirs = file_type_info["paths"]
    parser_files = []
    for extension in file_type_info["extensions"]:
        source_files = get_source_files(
            project, dirs, extension, parse_ft, saved_files, ignore_spec
        )
        for sf in source_files:
            files[sf.file_id] = sf
            parser_files.append(sf.file_id)
    return parser_files


def generate_dbt_ignore_spec(project_root):
    ignore_file_path = os.path.join(project_root, ".dbtignore")

    ignore_spec = None
    if os.path.exists(ignore_file_path):
        with open(ignore_file_path) as f:
            ignore_spec = pathspec.PathSpec.from_lines(pathspec.patterns.GitWildMatchPattern, f)
    return ignore_spec


# Protocol for the ReadFiles... classes
class ReadFiles(Protocol):
    files: MutableMapping[str, AnySourceFile]
    project_parser_files: Dict

    def read_files(self):
        pass


@dataclass
class ReadFilesFromFileSystem:
    all_projects: Mapping[str, Project]
    files: MutableMapping[str, AnySourceFile] = field(default_factory=dict)
    # saved_files is only used to compare schema files
    saved_files: MutableMapping[str, AnySourceFile] = field(default_factory=dict)
    # project_parser_files = {
    #   "my_project": {
    #     "ModelParser": ["my_project://models/my_model.sql"]
    #   }
    # }
    #
    project_parser_files: Dict = field(default_factory=dict)

    def read_files(self):
        for project in self.all_projects.values():
            file_types = get_file_types_for_project(project)
            self.read_files_for_project(project, file_types)

    def read_files_for_project(self, project, file_types):
        dbt_ignore_spec = generate_dbt_ignore_spec(project.project_root)
        project_files = self.project_parser_files[project.project_name] = {}

        for parse_ft, file_type_info in file_types.items():
            project_files[file_type_info["parser"]] = read_files_for_parser(
                project,
                self.files,
                parse_ft,
                file_type_info,
                self.saved_files,
                dbt_ignore_spec,
            )


@dataclass
class ReadFilesFromDiff:
    root_project_name: str
    all_projects: Mapping[str, Project]
    file_diff: FileDiff
    files: MutableMapping[str, AnySourceFile] = field(default_factory=dict)
    # saved_files is used to construct a fresh copy of files, without
    # additional information from parsing
    saved_files: MutableMapping[str, AnySourceFile] = field(default_factory=dict)
    project_parser_files: Dict = field(default_factory=dict)
    project_file_types: Dict = field(default_factory=dict)
    local_package_dirs: Optional[List[str]] = None

    def read_files(self):
        # Copy the base file information from the existing manifest.
        # We will do deletions, adds, changes from the file_diff to emulate
        # a complete read of the project file system.
        for file_id, source_file in self.saved_files.items():
            if isinstance(source_file, SchemaSourceFile):
                file_cls = SchemaSourceFile
            else:
                file_cls = SourceFile
            new_source_file = file_cls(
                path=source_file.path,
                checksum=source_file.checksum,
                project_name=source_file.project_name,
                parse_file_type=source_file.parse_file_type,
                contents=source_file.contents,
            )
            self.files[file_id] = new_source_file

        # Now that we have a copy of the files, remove deleted files
        # For now, we assume that all files are in the root_project, until
        # we've determined whether project name will be provided or deduced
        # from the directory.
        for input_file_path in self.file_diff.deleted:
            project_name = self.get_project_name(input_file_path)
            file_id = f"{project_name}://{input_file_path}"
            if file_id in self.files:
                self.files.pop(file_id)
            else:
                fire_event(InputFileDiffError(category="deleted file not found", file_id=file_id))

        # Now we do the changes
        for input_file in self.file_diff.changed:
            project_name = self.get_project_name(input_file.path)
            file_id = f"{project_name}://{input_file.path}"
            if file_id in self.files:
                # Get the existing source_file object and update the contents and mod time
                source_file = self.files[file_id]
                source_file.contents = input_file.content
                source_file.checksum = FileHash.from_contents(input_file.content)
                source_file.path.modification_time = input_file.modification_time
                # Handle creation of dictionary version of schema file content
                if isinstance(source_file, SchemaSourceFile) and source_file.contents:
                    dfy = yaml_from_file(source_file)
                    if dfy:
                        validate_yaml(source_file.path.original_file_path, dfy)
                        source_file.dfy = dfy
                    # TODO: ensure we have a file object even for empty files, such as schema files

        # Now the new files
        for input_file in self.file_diff.added:
            project_name = self.get_project_name(input_file.path)
            # FilePath
            #   searched_path  i.e. "models"
            #   relative_path  i.e. the part after searched_path, or "model.sql"
            #   modification_time  float, default 0.0...
            #   project_root
            # We use PurePath because there's no actual filesystem to look at
            input_file_path = pathlib.PurePath(input_file.path)
            extension = input_file_path.suffix
            searched_path = input_file_path.parts[0]
            # check what happens with generic tests... searched_path/relative_path

            relative_path_parts = input_file_path.parts[1:]
            relative_path = pathlib.PurePath("").joinpath(*relative_path_parts)
            # Create FilePath object
            input_file_path = FilePath(
                searched_path=searched_path,
                relative_path=str(relative_path),
                modification_time=input_file.modification_time,
                project_root=self.all_projects[project_name].project_root,
            )

            # Now use the extension and "searched_path" to determine which file_type
            (file_types, file_type_lookup) = self.get_project_file_types(project_name)
            parse_ft_for_extension = set()
            parse_ft_for_path = set()
            if extension in file_type_lookup["extensions"]:
                parse_ft_for_extension = file_type_lookup["extensions"][extension]
            if searched_path in file_type_lookup["paths"]:
                parse_ft_for_path = file_type_lookup["paths"][searched_path]
            if len(parse_ft_for_extension) == 0 or len(parse_ft_for_path) == 0:
                fire_event(InputFileDiffError(category="not a project file", file_id=file_id))
                continue
            parse_ft_set = parse_ft_for_extension.intersection(parse_ft_for_path)
            if (
                len(parse_ft_set) != 1
            ):  # There should only be one result for a path/extension combination
                fire_event(
                    InputFileDiffError(
                        category="unable to resolve diff file location", file_id=file_id
                    )
                )
                continue
            parse_ft = parse_ft_set.pop()
            source_file_cls = SourceFile
            if parse_ft == ParseFileType.Schema:
                source_file_cls = SchemaSourceFile
            source_file = source_file_cls(
                path=input_file_path,
                contents=input_file.content,
                checksum=FileHash.from_contents(input_file.content),
                project_name=project_name,
                parse_file_type=parse_ft,
            )
            if source_file_cls == SchemaSourceFile:
                dfy = yaml_from_file(source_file)
                if dfy:
                    validate_yaml(source_file.path.original_file_path, dfy)
                    source_file.dfy = dfy
                else:
                    # don't include in files because no content
                    continue
            self.files[source_file.file_id] = source_file

    def get_project_name(self, path):
        # It's not currently possible to recognize any other project files,
        # and it's an open issue how to handle deps.
        return self.root_project_name

    def get_project_file_types(self, project_name):
        if project_name not in self.project_file_types:
            file_types = get_file_types_for_project(self.all_projects[project_name])
            file_type_lookup = self.get_file_type_lookup(file_types)
            self.project_file_types[project_name] = {
                "file_types": file_types,
                "file_type_lookup": file_type_lookup,
            }
        file_types = self.project_file_types[project_name]["file_types"]
        file_type_lookup = self.project_file_types[project_name]["file_type_lookup"]
        return (file_types, file_type_lookup)

    def get_file_type_lookup(self, file_types):
        file_type_lookup = {"paths": {}, "extensions": {}}
        for parse_ft, file_type in file_types.items():
            for path in file_type["paths"]:
                if path not in file_type_lookup["paths"]:
                    file_type_lookup["paths"][path] = set()
                file_type_lookup["paths"][path].add(parse_ft)
            for extension in file_type["extensions"]:
                if extension not in file_type_lookup["extensions"]:
                    file_type_lookup["extensions"][extension] = set()
                file_type_lookup["extensions"][extension].add(parse_ft)
        return file_type_lookup


def get_file_types_for_project(project):
    file_types = {
        ParseFileType.Macro: {
            "paths": project.macro_paths,
            "extensions": [".sql"],
            "parser": "MacroParser",
        },
        ParseFileType.Model: {
            "paths": project.model_paths,
            "extensions": [".sql", ".py"],
            "parser": "ModelParser",
        },
        ParseFileType.Snapshot: {
            "paths": project.snapshot_paths,
            "extensions": [".sql"],
            "parser": "SnapshotParser",
        },
        ParseFileType.Analysis: {
            "paths": project.analysis_paths,
            "extensions": [".sql"],
            "parser": "AnalysisParser",
        },
        ParseFileType.SingularTest: {
            "paths": project.test_paths,
            "extensions": [".sql"],
            "parser": "SingularTestParser",
        },
        ParseFileType.GenericTest: {
            "paths": project.generic_test_paths,
            "extensions": [".sql"],
            "parser": "GenericTestParser",
        },
        ParseFileType.Seed: {
            "paths": project.seed_paths,
            "extensions": [".csv"],
            "parser": "SeedParser",
        },
        ParseFileType.Documentation: {
            "paths": project.docs_paths,
            "extensions": [".md"],
            "parser": "DocumentationParser",
        },
        ParseFileType.Schema: {
            "paths": project.all_source_paths,
            "extensions": [".yml", ".yaml"],
            "parser": "SchemaParser",
        },
        ParseFileType.Fixture: {
            "paths": project.fixture_paths,
            "extensions": [".csv", ".sql"],
            "parser": "FixtureParser",
        },
    }
    return file_types
