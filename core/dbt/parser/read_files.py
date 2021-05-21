from dbt.clients.system import load_file_contents
from dbt.contracts.files import FilePath, ParseFileType, SourceFile, FileHash

from dbt.parser.schemas import yaml_from_file
from dbt.parser.search import FilesystemSearcher


# This loads the files contents and creates the SourceFile object
def load_source_file(
        path: FilePath, parse_file_type: ParseFileType,
        project_name: str) -> SourceFile:
    file_contents = load_file_contents(path.absolute_path, strip=False)
    checksum = FileHash.from_contents(file_contents)
    source_file = SourceFile(path=path, checksum=checksum,
                             parse_file_type=parse_file_type, project_name=project_name)
    source_file.contents = file_contents.strip()
    if parse_file_type == ParseFileType.Schema:
        source_file.dict_from_yaml = yaml_from_file(source_file)
    return source_file


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
def get_source_files(project, paths, extension, parse_file_type):
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
            fb_list.append(load_source_file(
                fp, parse_file_type, project.project_name))
    return fb_list


def read_files_for_parser(project, files, dirs, extension, parse_ft):
    parser_files = []
    source_files = get_source_files(
        project, dirs, extension, parse_ft
    )
    for sf in source_files:
        files[sf.search_key] = sf
        parser_files.append(sf.search_key)
    return parser_files


# This needs to read files for multiple projects, so the 'files'
# dictionary needs to be passed in. What determines the order of
# the various projects? Is the root project always last? Do the
# non-root projects need to be done separately in order?
def read_files(project, files, parser_files):

    project_files = {}

    project_files['MacroParser'] = read_files_for_parser(
        project, files, project.macro_paths, '.sql', ParseFileType.Macro,
    )

    project_files['ModelParser'] = read_files_for_parser(
        project, files, project.source_paths, '.sql', ParseFileType.Model,
    )

    project_files['SnapshotParser'] = read_files_for_parser(
        project, files, project.snapshot_paths, '.sql', ParseFileType.Snapshot,
    )

    project_files['AnalysisParser'] = read_files_for_parser(
        project, files, project.analysis_paths, '.sql', ParseFileType.Analysis,
    )

    project_files['DataTestParser'] = read_files_for_parser(
        project, files, project.test_paths, '.sql', ParseFileType.Test,
    )

    project_files['SeedParser'] = read_files_for_parser(
        project, files, project.data_paths, '.csv', ParseFileType.Seed,
    )

    project_files['DocumentationParser'] = read_files_for_parser(
        project, files, project.docs_paths, '.md', ParseFileType.Documentation,
    )

    project_files['SchemaParser'] = read_files_for_parser(
        project, files, project.all_source_paths, '.yml', ParseFileType.Schema,
    )

    # Also read .yaml files for schema files. Might be better to change
    # 'read_files_for_parser' accept an array in the future.
    yaml_files = read_files_for_parser(
        project, files, project.all_source_paths, '.yaml', ParseFileType.Schema,
    )
    project_files['SchemaParser'].extend(yaml_files)

    # Store the parser files for this particular project
    parser_files[project.project_name] = project_files
