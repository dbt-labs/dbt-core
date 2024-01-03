# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: types.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import struct_pb2 as google_dot_protobuf_dot_struct__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0btypes.proto\x12\x0bproto_types\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1cgoogle/protobuf/struct.proto\"\x91\x02\n\tEventInfo\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0c\n\x04\x63ode\x18\x02 \x01(\t\x12\x0b\n\x03msg\x18\x03 \x01(\t\x12\r\n\x05level\x18\x04 \x01(\t\x12\x15\n\rinvocation_id\x18\x05 \x01(\t\x12\x0b\n\x03pid\x18\x06 \x01(\x05\x12\x0e\n\x06thread\x18\x07 \x01(\t\x12&\n\x02ts\x18\x08 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x30\n\x05\x65xtra\x18\t \x03(\x0b\x32!.proto_types.EventInfo.ExtraEntry\x12\x10\n\x08\x63\x61tegory\x18\n \x01(\t\x1a,\n\nExtraEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x7f\n\rTimingInfoMsg\x12\x0c\n\x04name\x18\x01 \x01(\t\x12.\n\nstarted_at\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x30\n\x0c\x63ompleted_at\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\"V\n\x0cNodeRelation\x12\x10\n\x08\x64\x61tabase\x18\n \x01(\t\x12\x0e\n\x06schema\x18\x0b \x01(\t\x12\r\n\x05\x61lias\x18\x0c \x01(\t\x12\x15\n\rrelation_name\x18\r \x01(\t\"\x91\x02\n\x08NodeInfo\x12\x11\n\tnode_path\x18\x01 \x01(\t\x12\x11\n\tnode_name\x18\x02 \x01(\t\x12\x11\n\tunique_id\x18\x03 \x01(\t\x12\x15\n\rresource_type\x18\x04 \x01(\t\x12\x14\n\x0cmaterialized\x18\x05 \x01(\t\x12\x13\n\x0bnode_status\x18\x06 \x01(\t\x12\x17\n\x0fnode_started_at\x18\x07 \x01(\t\x12\x18\n\x10node_finished_at\x18\x08 \x01(\t\x12%\n\x04meta\x18\t \x01(\x0b\x32\x17.google.protobuf.Struct\x12\x30\n\rnode_relation\x18\n \x01(\x0b\x32\x19.proto_types.NodeRelation\"6\n\x0eGenericMessage\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\"1\n\x11RetryExternalCall\x12\x0f\n\x07\x61ttempt\x18\x01 \x01(\x05\x12\x0b\n\x03max\x18\x02 \x01(\x05\"j\n\x14RetryExternalCallMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12,\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1e.proto_types.RetryExternalCall\"#\n\x14RecordRetryException\x12\x0b\n\x03\x65xc\x18\x01 \x01(\t\"p\n\x17RecordRetryExceptionMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12/\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32!.proto_types.RecordRetryException\"\x17\n\x15MainKeyboardInterrupt\"r\n\x18MainKeyboardInterruptMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\x30\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\".proto_types.MainKeyboardInterrupt\"#\n\x14MainEncounteredError\x12\x0b\n\x03\x65xc\x18\x01 \x01(\t\"p\n\x17MainEncounteredErrorMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12/\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32!.proto_types.MainEncounteredError\"%\n\x0eMainStackTrace\x12\x13\n\x0bstack_trace\x18\x01 \x01(\t\"d\n\x11MainStackTraceMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.MainStackTrace\"@\n\x13SystemCouldNotWrite\x12\x0c\n\x04path\x18\x01 \x01(\t\x12\x0e\n\x06reason\x18\x02 \x01(\t\x12\x0b\n\x03\x65xc\x18\x03 \x01(\t\"n\n\x16SystemCouldNotWriteMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12.\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32 .proto_types.SystemCouldNotWrite\"!\n\x12SystemExecutingCmd\x12\x0b\n\x03\x63md\x18\x01 \x03(\t\"l\n\x15SystemExecutingCmdMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12-\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1f.proto_types.SystemExecutingCmd\"\x1c\n\x0cSystemStdOut\x12\x0c\n\x04\x62msg\x18\x01 \x01(\t\"`\n\x0fSystemStdOutMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\'\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x19.proto_types.SystemStdOut\"\x1c\n\x0cSystemStdErr\x12\x0c\n\x04\x62msg\x18\x01 \x01(\t\"`\n\x0fSystemStdErrMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\'\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x19.proto_types.SystemStdErr\",\n\x16SystemReportReturnCode\x12\x12\n\nreturncode\x18\x01 \x01(\x05\"t\n\x19SystemReportReturnCodeMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\x31\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32#.proto_types.SystemReportReturnCode\"p\n\x13TimingInfoCollected\x12(\n\tnode_info\x18\x01 \x01(\x0b\x32\x15.proto_types.NodeInfo\x12/\n\x0btiming_info\x18\x02 \x01(\x0b\x32\x1a.proto_types.TimingInfoMsg\"n\n\x16TimingInfoCollectedMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12.\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32 .proto_types.TimingInfoCollected\"&\n\x12LogDebugStackTrace\x12\x10\n\x08\x65xc_info\x18\x01 \x01(\t\"l\n\x15LogDebugStackTraceMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12-\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1f.proto_types.LogDebugStackTrace\"\x1e\n\x0e\x43heckCleanPath\x12\x0c\n\x04path\x18\x01 \x01(\t\"d\n\x11\x43heckCleanPathMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.CheckCleanPath\" \n\x10\x43onfirmCleanPath\x12\x0c\n\x04path\x18\x01 \x01(\t\"h\n\x13\x43onfirmCleanPathMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12+\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1d.proto_types.ConfirmCleanPath\"\"\n\x12ProtectedCleanPath\x12\x0c\n\x04path\x18\x01 \x01(\t\"l\n\x15ProtectedCleanPathMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12-\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1f.proto_types.ProtectedCleanPath\"\x14\n\x12\x46inishedCleanPaths\"l\n\x15\x46inishedCleanPathsMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12-\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1f.proto_types.FinishedCleanPaths\"5\n\x0bOpenCommand\x12\x10\n\x08open_cmd\x18\x01 \x01(\t\x12\x14\n\x0cprofiles_dir\x18\x02 \x01(\t\"^\n\x0eOpenCommandMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12&\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x18.proto_types.OpenCommand\"\x19\n\nFormatting\x12\x0b\n\x03msg\x18\x01 \x01(\t\"\\\n\rFormattingMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12%\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x17.proto_types.Formatting\"0\n\x0fServingDocsPort\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\x05\"f\n\x12ServingDocsPortMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12*\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1c.proto_types.ServingDocsPort\"%\n\x15ServingDocsAccessInfo\x12\x0c\n\x04port\x18\x01 \x01(\t\"r\n\x18ServingDocsAccessInfoMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\x30\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\".proto_types.ServingDocsAccessInfo\"\x15\n\x13ServingDocsExitInfo\"n\n\x16ServingDocsExitInfoMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12.\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32 .proto_types.ServingDocsExitInfo\"J\n\x10RunResultWarning\x12\x15\n\rresource_type\x18\x01 \x01(\t\x12\x11\n\tnode_name\x18\x02 \x01(\t\x12\x0c\n\x04path\x18\x03 \x01(\t\"h\n\x13RunResultWarningMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12+\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1d.proto_types.RunResultWarning\"J\n\x10RunResultFailure\x12\x15\n\rresource_type\x18\x01 \x01(\t\x12\x11\n\tnode_name\x18\x02 \x01(\t\x12\x0c\n\x04path\x18\x03 \x01(\t\"h\n\x13RunResultFailureMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12+\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1d.proto_types.RunResultFailure\"k\n\tStatsLine\x12\x30\n\x05stats\x18\x01 \x03(\x0b\x32!.proto_types.StatsLine.StatsEntry\x1a,\n\nStatsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x05:\x02\x38\x01\"Z\n\x0cStatsLineMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12$\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x16.proto_types.StatsLine\"\x1d\n\x0eRunResultError\x12\x0b\n\x03msg\x18\x01 \x01(\t\"d\n\x11RunResultErrorMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.RunResultError\")\n\x17RunResultErrorNoMessage\x12\x0e\n\x06status\x18\x01 \x01(\t\"v\n\x1aRunResultErrorNoMessageMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\x32\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32$.proto_types.RunResultErrorNoMessage\"\x1f\n\x0fSQLCompiledPath\x12\x0c\n\x04path\x18\x01 \x01(\t\"f\n\x12SQLCompiledPathMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12*\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1c.proto_types.SQLCompiledPath\"-\n\x14\x43heckNodeTestFailure\x12\x15\n\rrelation_name\x18\x01 \x01(\t\"p\n\x17\x43heckNodeTestFailureMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12/\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32!.proto_types.CheckNodeTestFailure\"W\n\x0f\x45ndOfRunSummary\x12\x12\n\nnum_errors\x18\x01 \x01(\x05\x12\x14\n\x0cnum_warnings\x18\x02 \x01(\x05\x12\x1a\n\x12keyboard_interrupt\x18\x03 \x01(\x08\"f\n\x12\x45ndOfRunSummaryMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12*\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1c.proto_types.EndOfRunSummary\"U\n\x13LogSkipBecauseError\x12\x0e\n\x06schema\x18\x01 \x01(\t\x12\x10\n\x08relation\x18\x02 \x01(\t\x12\r\n\x05index\x18\x03 \x01(\x05\x12\r\n\x05total\x18\x04 \x01(\x05\"n\n\x16LogSkipBecauseErrorMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12.\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32 .proto_types.LogSkipBecauseError\"\x14\n\x12\x45nsureGitInstalled\"l\n\x15\x45nsureGitInstalledMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12-\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1f.proto_types.EnsureGitInstalled\"\x1a\n\x18\x44\x65psCreatingLocalSymlink\"x\n\x1b\x44\x65psCreatingLocalSymlinkMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\x33\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32%.proto_types.DepsCreatingLocalSymlink\"\x19\n\x17\x44\x65psSymlinkNotAvailable\"v\n\x1a\x44\x65psSymlinkNotAvailableMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\x32\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32$.proto_types.DepsSymlinkNotAvailable\"\x11\n\x0f\x44isableTracking\"f\n\x12\x44isableTrackingMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12*\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1c.proto_types.DisableTracking\"\x1e\n\x0cSendingEvent\x12\x0e\n\x06kwargs\x18\x01 \x01(\t\"`\n\x0fSendingEventMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\'\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x19.proto_types.SendingEvent\"\x12\n\x10SendEventFailure\"h\n\x13SendEventFailureMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12+\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1d.proto_types.SendEventFailure\"\r\n\x0b\x46lushEvents\"^\n\x0e\x46lushEventsMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12&\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x18.proto_types.FlushEvents\"\x14\n\x12\x46lushEventsFailure\"l\n\x15\x46lushEventsFailureMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12-\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1f.proto_types.FlushEventsFailure\"-\n\x19TrackingInitializeFailure\x12\x10\n\x08\x65xc_info\x18\x01 \x01(\t\"z\n\x1cTrackingInitializeFailureMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\x34\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32&.proto_types.TrackingInitializeFailure\"&\n\x17RunResultWarningMessage\x12\x0b\n\x03msg\x18\x01 \x01(\t\"v\n\x1aRunResultWarningMessageMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\x32\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32$.proto_types.RunResultWarningMessage\"\x1a\n\x0b\x44\x65\x62ugCmdOut\x12\x0b\n\x03msg\x18\x01 \x01(\t\"^\n\x0e\x44\x65\x62ugCmdOutMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12&\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x18.proto_types.DebugCmdOut\"\x1d\n\x0e\x44\x65\x62ugCmdResult\x12\x0b\n\x03msg\x18\x01 \x01(\t\"d\n\x11\x44\x65\x62ugCmdResultMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.DebugCmdResult\"\x19\n\nListCmdOut\x12\x0b\n\x03msg\x18\x01 \x01(\t\"\\\n\rListCmdOutMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12%\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x17.proto_types.ListCmdOut\"\x13\n\x04Note\x12\x0b\n\x03msg\x18\x01 \x01(\t\"P\n\x07NoteMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12\x1f\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x11.proto_types.Note\"\xec\x01\n\x0eResourceReport\x12\x14\n\x0c\x63ommand_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ommand_success\x18\x03 \x01(\x08\x12\x1f\n\x17\x63ommand_wall_clock_time\x18\x04 \x01(\x02\x12\x19\n\x11process_user_time\x18\x05 \x01(\x02\x12\x1b\n\x13process_kernel_time\x18\x06 \x01(\x02\x12\x1b\n\x13process_mem_max_rss\x18\x07 \x01(\x03\x12\x19\n\x11process_in_blocks\x18\x08 \x01(\x03\x12\x1a\n\x12process_out_blocks\x18\t \x01(\x03\"d\n\x11ResourceReportMsg\x12$\n\x04info\x18\x01 \x01(\x0b\x32\x16.proto_types.EventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.ResourceReportb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'types_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _EVENTINFO_EXTRAENTRY._options = None
  _EVENTINFO_EXTRAENTRY._serialized_options = b'8\001'
  _STATSLINE_STATSENTRY._options = None
  _STATSLINE_STATSENTRY._serialized_options = b'8\001'
  _globals['_EVENTINFO']._serialized_start=92
  _globals['_EVENTINFO']._serialized_end=365
  _globals['_EVENTINFO_EXTRAENTRY']._serialized_start=321
  _globals['_EVENTINFO_EXTRAENTRY']._serialized_end=365
  _globals['_TIMINGINFOMSG']._serialized_start=367
  _globals['_TIMINGINFOMSG']._serialized_end=494
  _globals['_NODERELATION']._serialized_start=496
  _globals['_NODERELATION']._serialized_end=582
  _globals['_NODEINFO']._serialized_start=585
  _globals['_NODEINFO']._serialized_end=858
  _globals['_GENERICMESSAGE']._serialized_start=860
  _globals['_GENERICMESSAGE']._serialized_end=914
  _globals['_RETRYEXTERNALCALL']._serialized_start=916
  _globals['_RETRYEXTERNALCALL']._serialized_end=965
  _globals['_RETRYEXTERNALCALLMSG']._serialized_start=967
  _globals['_RETRYEXTERNALCALLMSG']._serialized_end=1073
  _globals['_RECORDRETRYEXCEPTION']._serialized_start=1075
  _globals['_RECORDRETRYEXCEPTION']._serialized_end=1110
  _globals['_RECORDRETRYEXCEPTIONMSG']._serialized_start=1112
  _globals['_RECORDRETRYEXCEPTIONMSG']._serialized_end=1224
  _globals['_MAINKEYBOARDINTERRUPT']._serialized_start=1226
  _globals['_MAINKEYBOARDINTERRUPT']._serialized_end=1249
  _globals['_MAINKEYBOARDINTERRUPTMSG']._serialized_start=1251
  _globals['_MAINKEYBOARDINTERRUPTMSG']._serialized_end=1365
  _globals['_MAINENCOUNTEREDERROR']._serialized_start=1367
  _globals['_MAINENCOUNTEREDERROR']._serialized_end=1402
  _globals['_MAINENCOUNTEREDERRORMSG']._serialized_start=1404
  _globals['_MAINENCOUNTEREDERRORMSG']._serialized_end=1516
  _globals['_MAINSTACKTRACE']._serialized_start=1518
  _globals['_MAINSTACKTRACE']._serialized_end=1555
  _globals['_MAINSTACKTRACEMSG']._serialized_start=1557
  _globals['_MAINSTACKTRACEMSG']._serialized_end=1657
  _globals['_SYSTEMCOULDNOTWRITE']._serialized_start=1659
  _globals['_SYSTEMCOULDNOTWRITE']._serialized_end=1723
  _globals['_SYSTEMCOULDNOTWRITEMSG']._serialized_start=1725
  _globals['_SYSTEMCOULDNOTWRITEMSG']._serialized_end=1835
  _globals['_SYSTEMEXECUTINGCMD']._serialized_start=1837
  _globals['_SYSTEMEXECUTINGCMD']._serialized_end=1870
  _globals['_SYSTEMEXECUTINGCMDMSG']._serialized_start=1872
  _globals['_SYSTEMEXECUTINGCMDMSG']._serialized_end=1980
  _globals['_SYSTEMSTDOUT']._serialized_start=1982
  _globals['_SYSTEMSTDOUT']._serialized_end=2010
  _globals['_SYSTEMSTDOUTMSG']._serialized_start=2012
  _globals['_SYSTEMSTDOUTMSG']._serialized_end=2108
  _globals['_SYSTEMSTDERR']._serialized_start=2110
  _globals['_SYSTEMSTDERR']._serialized_end=2138
  _globals['_SYSTEMSTDERRMSG']._serialized_start=2140
  _globals['_SYSTEMSTDERRMSG']._serialized_end=2236
  _globals['_SYSTEMREPORTRETURNCODE']._serialized_start=2238
  _globals['_SYSTEMREPORTRETURNCODE']._serialized_end=2282
  _globals['_SYSTEMREPORTRETURNCODEMSG']._serialized_start=2284
  _globals['_SYSTEMREPORTRETURNCODEMSG']._serialized_end=2400
  _globals['_TIMINGINFOCOLLECTED']._serialized_start=2402
  _globals['_TIMINGINFOCOLLECTED']._serialized_end=2514
  _globals['_TIMINGINFOCOLLECTEDMSG']._serialized_start=2516
  _globals['_TIMINGINFOCOLLECTEDMSG']._serialized_end=2626
  _globals['_LOGDEBUGSTACKTRACE']._serialized_start=2628
  _globals['_LOGDEBUGSTACKTRACE']._serialized_end=2666
  _globals['_LOGDEBUGSTACKTRACEMSG']._serialized_start=2668
  _globals['_LOGDEBUGSTACKTRACEMSG']._serialized_end=2776
  _globals['_CHECKCLEANPATH']._serialized_start=2778
  _globals['_CHECKCLEANPATH']._serialized_end=2808
  _globals['_CHECKCLEANPATHMSG']._serialized_start=2810
  _globals['_CHECKCLEANPATHMSG']._serialized_end=2910
  _globals['_CONFIRMCLEANPATH']._serialized_start=2912
  _globals['_CONFIRMCLEANPATH']._serialized_end=2944
  _globals['_CONFIRMCLEANPATHMSG']._serialized_start=2946
  _globals['_CONFIRMCLEANPATHMSG']._serialized_end=3050
  _globals['_PROTECTEDCLEANPATH']._serialized_start=3052
  _globals['_PROTECTEDCLEANPATH']._serialized_end=3086
  _globals['_PROTECTEDCLEANPATHMSG']._serialized_start=3088
  _globals['_PROTECTEDCLEANPATHMSG']._serialized_end=3196
  _globals['_FINISHEDCLEANPATHS']._serialized_start=3198
  _globals['_FINISHEDCLEANPATHS']._serialized_end=3218
  _globals['_FINISHEDCLEANPATHSMSG']._serialized_start=3220
  _globals['_FINISHEDCLEANPATHSMSG']._serialized_end=3328
  _globals['_OPENCOMMAND']._serialized_start=3330
  _globals['_OPENCOMMAND']._serialized_end=3383
  _globals['_OPENCOMMANDMSG']._serialized_start=3385
  _globals['_OPENCOMMANDMSG']._serialized_end=3479
  _globals['_FORMATTING']._serialized_start=3481
  _globals['_FORMATTING']._serialized_end=3506
  _globals['_FORMATTINGMSG']._serialized_start=3508
  _globals['_FORMATTINGMSG']._serialized_end=3600
  _globals['_SERVINGDOCSPORT']._serialized_start=3602
  _globals['_SERVINGDOCSPORT']._serialized_end=3650
  _globals['_SERVINGDOCSPORTMSG']._serialized_start=3652
  _globals['_SERVINGDOCSPORTMSG']._serialized_end=3754
  _globals['_SERVINGDOCSACCESSINFO']._serialized_start=3756
  _globals['_SERVINGDOCSACCESSINFO']._serialized_end=3793
  _globals['_SERVINGDOCSACCESSINFOMSG']._serialized_start=3795
  _globals['_SERVINGDOCSACCESSINFOMSG']._serialized_end=3909
  _globals['_SERVINGDOCSEXITINFO']._serialized_start=3911
  _globals['_SERVINGDOCSEXITINFO']._serialized_end=3932
  _globals['_SERVINGDOCSEXITINFOMSG']._serialized_start=3934
  _globals['_SERVINGDOCSEXITINFOMSG']._serialized_end=4044
  _globals['_RUNRESULTWARNING']._serialized_start=4046
  _globals['_RUNRESULTWARNING']._serialized_end=4120
  _globals['_RUNRESULTWARNINGMSG']._serialized_start=4122
  _globals['_RUNRESULTWARNINGMSG']._serialized_end=4226
  _globals['_RUNRESULTFAILURE']._serialized_start=4228
  _globals['_RUNRESULTFAILURE']._serialized_end=4302
  _globals['_RUNRESULTFAILUREMSG']._serialized_start=4304
  _globals['_RUNRESULTFAILUREMSG']._serialized_end=4408
  _globals['_STATSLINE']._serialized_start=4410
  _globals['_STATSLINE']._serialized_end=4517
  _globals['_STATSLINE_STATSENTRY']._serialized_start=4473
  _globals['_STATSLINE_STATSENTRY']._serialized_end=4517
  _globals['_STATSLINEMSG']._serialized_start=4519
  _globals['_STATSLINEMSG']._serialized_end=4609
  _globals['_RUNRESULTERROR']._serialized_start=4611
  _globals['_RUNRESULTERROR']._serialized_end=4640
  _globals['_RUNRESULTERRORMSG']._serialized_start=4642
  _globals['_RUNRESULTERRORMSG']._serialized_end=4742
  _globals['_RUNRESULTERRORNOMESSAGE']._serialized_start=4744
  _globals['_RUNRESULTERRORNOMESSAGE']._serialized_end=4785
  _globals['_RUNRESULTERRORNOMESSAGEMSG']._serialized_start=4787
  _globals['_RUNRESULTERRORNOMESSAGEMSG']._serialized_end=4905
  _globals['_SQLCOMPILEDPATH']._serialized_start=4907
  _globals['_SQLCOMPILEDPATH']._serialized_end=4938
  _globals['_SQLCOMPILEDPATHMSG']._serialized_start=4940
  _globals['_SQLCOMPILEDPATHMSG']._serialized_end=5042
  _globals['_CHECKNODETESTFAILURE']._serialized_start=5044
  _globals['_CHECKNODETESTFAILURE']._serialized_end=5089
  _globals['_CHECKNODETESTFAILUREMSG']._serialized_start=5091
  _globals['_CHECKNODETESTFAILUREMSG']._serialized_end=5203
  _globals['_ENDOFRUNSUMMARY']._serialized_start=5205
  _globals['_ENDOFRUNSUMMARY']._serialized_end=5292
  _globals['_ENDOFRUNSUMMARYMSG']._serialized_start=5294
  _globals['_ENDOFRUNSUMMARYMSG']._serialized_end=5396
  _globals['_LOGSKIPBECAUSEERROR']._serialized_start=5398
  _globals['_LOGSKIPBECAUSEERROR']._serialized_end=5483
  _globals['_LOGSKIPBECAUSEERRORMSG']._serialized_start=5485
  _globals['_LOGSKIPBECAUSEERRORMSG']._serialized_end=5595
  _globals['_ENSUREGITINSTALLED']._serialized_start=5597
  _globals['_ENSUREGITINSTALLED']._serialized_end=5617
  _globals['_ENSUREGITINSTALLEDMSG']._serialized_start=5619
  _globals['_ENSUREGITINSTALLEDMSG']._serialized_end=5727
  _globals['_DEPSCREATINGLOCALSYMLINK']._serialized_start=5729
  _globals['_DEPSCREATINGLOCALSYMLINK']._serialized_end=5755
  _globals['_DEPSCREATINGLOCALSYMLINKMSG']._serialized_start=5757
  _globals['_DEPSCREATINGLOCALSYMLINKMSG']._serialized_end=5877
  _globals['_DEPSSYMLINKNOTAVAILABLE']._serialized_start=5879
  _globals['_DEPSSYMLINKNOTAVAILABLE']._serialized_end=5904
  _globals['_DEPSSYMLINKNOTAVAILABLEMSG']._serialized_start=5906
  _globals['_DEPSSYMLINKNOTAVAILABLEMSG']._serialized_end=6024
  _globals['_DISABLETRACKING']._serialized_start=6026
  _globals['_DISABLETRACKING']._serialized_end=6043
  _globals['_DISABLETRACKINGMSG']._serialized_start=6045
  _globals['_DISABLETRACKINGMSG']._serialized_end=6147
  _globals['_SENDINGEVENT']._serialized_start=6149
  _globals['_SENDINGEVENT']._serialized_end=6179
  _globals['_SENDINGEVENTMSG']._serialized_start=6181
  _globals['_SENDINGEVENTMSG']._serialized_end=6277
  _globals['_SENDEVENTFAILURE']._serialized_start=6279
  _globals['_SENDEVENTFAILURE']._serialized_end=6297
  _globals['_SENDEVENTFAILUREMSG']._serialized_start=6299
  _globals['_SENDEVENTFAILUREMSG']._serialized_end=6403
  _globals['_FLUSHEVENTS']._serialized_start=6405
  _globals['_FLUSHEVENTS']._serialized_end=6418
  _globals['_FLUSHEVENTSMSG']._serialized_start=6420
  _globals['_FLUSHEVENTSMSG']._serialized_end=6514
  _globals['_FLUSHEVENTSFAILURE']._serialized_start=6516
  _globals['_FLUSHEVENTSFAILURE']._serialized_end=6536
  _globals['_FLUSHEVENTSFAILUREMSG']._serialized_start=6538
  _globals['_FLUSHEVENTSFAILUREMSG']._serialized_end=6646
  _globals['_TRACKINGINITIALIZEFAILURE']._serialized_start=6648
  _globals['_TRACKINGINITIALIZEFAILURE']._serialized_end=6693
  _globals['_TRACKINGINITIALIZEFAILUREMSG']._serialized_start=6695
  _globals['_TRACKINGINITIALIZEFAILUREMSG']._serialized_end=6817
  _globals['_RUNRESULTWARNINGMESSAGE']._serialized_start=6819
  _globals['_RUNRESULTWARNINGMESSAGE']._serialized_end=6857
  _globals['_RUNRESULTWARNINGMESSAGEMSG']._serialized_start=6859
  _globals['_RUNRESULTWARNINGMESSAGEMSG']._serialized_end=6977
  _globals['_DEBUGCMDOUT']._serialized_start=6979
  _globals['_DEBUGCMDOUT']._serialized_end=7005
  _globals['_DEBUGCMDOUTMSG']._serialized_start=7007
  _globals['_DEBUGCMDOUTMSG']._serialized_end=7101
  _globals['_DEBUGCMDRESULT']._serialized_start=7103
  _globals['_DEBUGCMDRESULT']._serialized_end=7132
  _globals['_DEBUGCMDRESULTMSG']._serialized_start=7134
  _globals['_DEBUGCMDRESULTMSG']._serialized_end=7234
  _globals['_LISTCMDOUT']._serialized_start=7236
  _globals['_LISTCMDOUT']._serialized_end=7261
  _globals['_LISTCMDOUTMSG']._serialized_start=7263
  _globals['_LISTCMDOUTMSG']._serialized_end=7355
  _globals['_NOTE']._serialized_start=7357
  _globals['_NOTE']._serialized_end=7376
  _globals['_NOTEMSG']._serialized_start=7378
  _globals['_NOTEMSG']._serialized_end=7458
  _globals['_RESOURCEREPORT']._serialized_start=7461
  _globals['_RESOURCEREPORT']._serialized_end=7697
  _globals['_RESOURCEREPORTMSG']._serialized_start=7699
  _globals['_RESOURCEREPORTMSG']._serialized_end=7799
# @@protoc_insertion_point(module_scope)
