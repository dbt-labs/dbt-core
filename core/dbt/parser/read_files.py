from dbt.clients.system import load_file_contents
from dbt.contracts.files import (
    FilePath, ParseFileType, SourceFile, FileHash, AnySourceFile, SchemaSourceFile
)

from dbt.parser.schemas import yaml_from_file, schema_file_keys, check_format_version
from dbt.exceptions import CompilationException
from dbt.parser.search import FilesystemSearcher
from typing import Optional


# This loads the files contents and creates the SourceFile object
def load_source_file(
        path: FilePath, parse_file_type: ParseFileType,
        project_name: str, saved_files,) -> Optional[AnySourceFile]:

    sf_cls = SchemaSourceFile if parse_file_type == ParseFileType.Schema else SourceFile
    source_file = sf_cls(path=path, checksum=FileHash.empty(),
                         parse_file_type=parse_file_type, project_name=project_name)

    skip_loading_schema_file = False
    if (parse_file_type == ParseFileType.Schema and
            saved_files and source_file.file_id in saved_files):
        old_source_file = saved_files[source_file.file_id]
        if (source_file.path.modification_time != 0.0 and
                old_source_file.path.modification_time == source_file.path.modification_time):
            source_file.checksum = old_source_file.checksum
            source_file.dfy = old_source_file.dfy
            skip_loading_schema_file = True

    if not skip_loading_schema_file:
        file_contents = load_file_contents(path.absolute_path, strip=False)
        source_file.checksum = FileHash.from_contents(file_contents)
        source_file.contents = file_contents.strip()

    if parse_file_type == ParseFileType.Schema and source_file.contents:
        dfy = yaml_from_file(source_file)
        if dfy:
            validate_yaml(source_file.path.original_file_path, dfy)
            source_file.dfy = dfy
        else:
            source_file = None
    return source_file


# Do some minimal validation of the yaml in a schema file.
# Check version, that key values are lists and that each element in
# the lists has a 'name' key
def validate_yaml(file_path, dct):
    check_format_version(file_path, dct)
    for key in schema_file_keys:
        if key in dct:
            if not isinstance(dct[key], list):
                msg = (f"The schema file at {file_path} is "
                       f"invalid because the value of '{key}' is not a list")
                raise CompilationException(msg)
            for element in dct[key]:
                if not isinstance(element, dict):
                    msg = (f"The schema file at {file_path} is "
                           f"invalid because a list element for '{key}' is not a dictionary")
                    raise CompilationException(msg)
                if 'name' not in element:
                    msg = (f"The schema file at {file_path} is "
                           f"invalid because a list element for '{key}' does not have a "
                           "name attribute.")
                    raise CompilationException(msg)


# Special processing for big seed files
def load_seed_source_file(match: FilePath, project_name) -> SourceFile:
    if match.seed_too_large():
        # We don't want to calculate a hash of this file. Use the path.
        source_file = SourceFile.big_seed(match)
    else:
        file_contents = load_file_contents(match.absolute_path, strip=False)
        checksum = FileHash.from_contents(file_contents)
        source_file = SourceFile(path=match, checksum=checksum)
        source_file.contents = ''
    source_file.parse_file_type = ParseFileType.Seed
    source_file.project_name = project_name
    return source_file


# Use the FilesystemSearcher to get a bunch of FilePaths, then turn
# them into a bunch of FileSource objects
def get_source_files(project, paths, extension, parse_file_type, saved_files):
    # file path list
    fp_list = list(FilesystemSearcher(
        project, paths, extension
    ))
    # file block list
    fb_list = []
    for fp in fp_list:
        if parse_file_type == ParseFileType.Seed:
            fb_list.append(load_seed_source_file(fp, project.project_name))
        else:
            file = load_source_file(fp, parse_file_type, project.project_name, saved_files)
            # only append the list if it has contents. added to fix #3568
            if file:
                fb_list.append(file)
    return fb_list


def read_files_for_parser(project, files, dirs, extension, parse_ft, saved_files):
    parser_files = []
    source_files = get_source_files(
        project, dirs, extension, parse_ft, saved_files
    )
    for sf in source_files:
        files[sf.file_id] = sf
        parser_files.append(sf.file_id)
    return parser_files


# This needs to read files for multiple projects, so the 'files'
# dictionary needs to be passed in. What determines the order of
# the various projects? Is the root project always last? Do the
# non-root projects need to be done separately in order?
def read_files(project, files, parser_files, saved_files):

    project_files = {}

    project_files['MacroParser'] = read_files_for_parser(
        project, files, project.macro_paths, '.sql', ParseFileType.Macro, saved_files
    )

    project_files['ModelParser'] = read_files_for_parser(
        project, files, project.model_paths, '.sql', ParseFileType.Model, saved_files
    )

    project_files['SnapshotParser'] = read_files_for_parser(
        project, files, project.snapshot_paths, '.sql', ParseFileType.Snapshot, saved_files
    )

    project_files['AnalysisParser'] = read_files_for_parser(
        project, files, project.analysis_paths, '.sql', ParseFileType.Analysis, saved_files
    )

    project_files['SingularTestParser'] = read_files_for_parser(
        project, files, project.test_paths, '.sql', ParseFileType.Test, saved_files
    )

    project_files['SeedParser'] = read_files_for_parser(
        project, files, project.seed_paths, '.csv', ParseFileType.Seed, saved_files
    )

    project_files['DocumentationParser'] = read_files_for_parser(
        project, files, project.docs_paths, '.md', ParseFileType.Documentation, saved_files
    )

    project_files['SchemaParser'] = read_files_for_parser(
        project, files, project.all_source_paths, '.yml', ParseFileType.Schema, saved_files
    )

    # Also read .yaml files for schema files. Might be better to change
    # 'read_files_for_parser' accept an array in the future.
    yaml_files = read_files_for_parser(
        project, files, project.all_source_paths, '.yaml', ParseFileType.Schema, saved_files
    )
    project_files['SchemaParser'].extend(yaml_files)

    # Store the parser files for this particular project
    parser_files[project.project_name] = project_files
