# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: messages.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0emessages.proto\"X\n\x0bStorageNode\x12\x0c\n\x04ipv4\x18\x01 \x01(\t\x12\x15\n\rport_get_data\x18\x02 \x01(\x05\x12\x17\n\x0fport_store_data\x18\x03 \x01(\x05\x12\x0b\n\x03uid\x18\x04 \x01(\t\"T\n\x1fStorageNodeAdvertisementRequest\x12\x1a\n\x04node\x18\x01 \x01(\x0b\x32\x0c.StorageNode\x12\x15\n\rfriendly_name\x18\x02 \x01(\t\"3\n StorageNodeAdvertisementResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"\"\n\x0eGetDataRequest\x12\x10\n\x08\x66ile_uid\x18\x01 \x01(\t\"5\n\x0fGetDataResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x11\n\tfile_data\x18\x02 \x01(\x0c\"7\n\x10StoreDataRequest\x12\x10\n\x08\x66ile_uid\x18\x01 \x01(\t\x12\x11\n\tfile_data\x18\x02 \x01(\x0c\"$\n\x11StoreDataResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"j\n\x18\x44\x65legateStoreDataRequest\x12\x10\n\x08\x66ile_uid\x18\x01 \x01(\t\x12\x11\n\tfile_data\x18\x02 \x01(\x0c\x12)\n\x13nodes_to_forward_to\x18\x03 \x03(\x0b\x32\x0c.StorageNode\",\n\x19\x44\x65legateStoreDataResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"\x94\x04\n\x07Message\x12\x16\n\x04type\x18\x01 \x01(\x0e\x32\x08.MsgType\x12+\n\x10get_data_request\x18\x02 \x01(\x0b\x32\x0f.GetDataRequestH\x00\x12-\n\x11get_data_response\x18\x03 \x01(\x0b\x32\x10.GetDataResponseH\x00\x12/\n\x12store_data_request\x18\x04 \x01(\x0b\x32\x11.StoreDataRequestH\x00\x12\x31\n\x13store_data_response\x18\x05 \x01(\x0b\x32\x12.StoreDataResponseH\x00\x12@\n\x1b\x64\x65legate_store_data_request\x18\x06 \x01(\x0b\x32\x19.DelegateStoreDataRequestH\x00\x12\x42\n\x1c\x64\x65legate_store_data_response\x18\x07 \x01(\x0b\x32\x1a.DelegateStoreDataResponseH\x00\x12N\n\"storage_node_advertisement_request\x18\x08 \x01(\x0b\x32 .StorageNodeAdvertisementRequestH\x00\x12P\n#storage_node_advertisement_response\x18\t \x01(\x0b\x32!.StorageNodeAdvertisementResponseH\x00\x42\t\n\x07payload*\xfb\x01\n\x07MsgType\x12\x14\n\x10GET_DATA_REQUEST\x10\x00\x12\x15\n\x11GET_DATA_RESPONSE\x10\x01\x12\x16\n\x12STORE_DATA_REQUEST\x10\x02\x12\x17\n\x13STORE_DATA_RESPONSE\x10\x03\x12\x1f\n\x1b\x44\x45LEGATE_STORE_DATA_REQUEST\x10\x04\x12 \n\x1c\x44\x45LEGATE_STORE_DATA_RESPONSE\x10\x05\x12&\n\"STORAGE_NODE_ADVERTISEMENT_REQUEST\x10\x06\x12\'\n#STORAGE_NODE_ADVERTISEMENT_RESPONSE\x10\x07\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'messages_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _MSGTYPE._serialized_start=1123
  _MSGTYPE._serialized_end=1374
  _STORAGENODE._serialized_start=18
  _STORAGENODE._serialized_end=106
  _STORAGENODEADVERTISEMENTREQUEST._serialized_start=108
  _STORAGENODEADVERTISEMENTREQUEST._serialized_end=192
  _STORAGENODEADVERTISEMENTRESPONSE._serialized_start=194
  _STORAGENODEADVERTISEMENTRESPONSE._serialized_end=245
  _GETDATAREQUEST._serialized_start=247
  _GETDATAREQUEST._serialized_end=281
  _GETDATARESPONSE._serialized_start=283
  _GETDATARESPONSE._serialized_end=336
  _STOREDATAREQUEST._serialized_start=338
  _STOREDATAREQUEST._serialized_end=393
  _STOREDATARESPONSE._serialized_start=395
  _STOREDATARESPONSE._serialized_end=431
  _DELEGATESTOREDATAREQUEST._serialized_start=433
  _DELEGATESTOREDATAREQUEST._serialized_end=539
  _DELEGATESTOREDATARESPONSE._serialized_start=541
  _DELEGATESTOREDATARESPONSE._serialized_end=585
  _MESSAGE._serialized_start=588
  _MESSAGE._serialized_end=1120
# @@protoc_insertion_point(module_scope)
