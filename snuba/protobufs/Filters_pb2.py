# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: snuba/protobufs/Filters.proto
# Protobuf Python Version: 5.27.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    27,
    1,
    '',
    'snuba/protobufs/Filters.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1dsnuba/protobufs/Filters.proto\".\n\tAndFilter\x12!\n\x07\x66ilters\x18\x01 \x03(\x0b\x32\x10.TraceItemFilter\"-\n\x08OrFilter\x12!\n\x07\x66ilters\x18\x01 \x03(\x0b\x32\x10.TraceItemFilter\"\xcb\x01\n\x0fNumericalFilter\x12\x10\n\x08tag_name\x18\x01 \x01(\t\x12\x1f\n\x02op\x18\x02 \x01(\x0e\x32\x13.NumericalFilter.Op\x12\r\n\x05value\x18\x03 \x01(\x02\"v\n\x02Op\x12\r\n\tLESS_THAN\x10\x00\x12\x10\n\x0cGREATER_THAN\x10\x01\x12\x17\n\x13LESS_THAN_OR_EQUALS\x10\x02\x12\x1a\n\x16GREATER_THAN_OR_EQUALS\x10\x03\x12\n\n\x06\x45QUALS\x10\x04\x12\x0e\n\nNOT_EQUALS\x10\x05\"\x87\x01\n\x0cStringFilter\x12\x10\n\x08tag_name\x18\x01 \x01(\t\x12\x1c\n\x02op\x18\x02 \x01(\x0e\x32\x10.StringFilter.Op\x12\r\n\x05value\x18\x03 \x01(\t\"8\n\x02Op\x12\n\n\x06\x45QUALS\x10\x00\x12\x0e\n\nNOT_EQUALS\x10\x01\x12\x08\n\x04LIKE\x10\x02\x12\x0c\n\x08NOT_LIKE\x10\x03\" \n\x0c\x45xistsFilter\x12\x10\n\x08tag_name\x18\x01 \x01(\t\"\xca\x01\n\x0fTraceItemFilter\x12\x19\n\x03\x61nd\x18\x01 \x01(\x0b\x32\n.AndFilterH\x00\x12\x17\n\x02or\x18\x02 \x01(\x0b\x32\t.OrFilterH\x00\x12-\n\x11number_comparison\x18\x03 \x01(\x0b\x32\x10.NumericalFilterH\x00\x12*\n\x11string_comparison\x18\x04 \x01(\x0b\x32\r.StringFilterH\x00\x12\x1f\n\x06\x65xists\x18\x05 \x01(\x0b\x32\r.ExistsFilterH\x00\x42\x07\n\x05valueb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'snuba.protobufs.Filters_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_ANDFILTER']._serialized_start=33
  _globals['_ANDFILTER']._serialized_end=79
  _globals['_ORFILTER']._serialized_start=81
  _globals['_ORFILTER']._serialized_end=126
  _globals['_NUMERICALFILTER']._serialized_start=129
  _globals['_NUMERICALFILTER']._serialized_end=332
  _globals['_NUMERICALFILTER_OP']._serialized_start=214
  _globals['_NUMERICALFILTER_OP']._serialized_end=332
  _globals['_STRINGFILTER']._serialized_start=335
  _globals['_STRINGFILTER']._serialized_end=470
  _globals['_STRINGFILTER_OP']._serialized_start=414
  _globals['_STRINGFILTER_OP']._serialized_end=470
  _globals['_EXISTSFILTER']._serialized_start=472
  _globals['_EXISTSFILTER']._serialized_end=504
  _globals['_TRACEITEMFILTER']._serialized_start=507
  _globals['_TRACEITEMFILTER']._serialized_end=709
# @@protoc_insertion_point(module_scope)
