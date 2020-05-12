// Code generated by protoc-gen-go. DO NOT EDIT.
// source: point.proto

package point

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type WritePoint struct {
	DataBase             string            `protobuf:"bytes,1,opt,name=dataBase,proto3" json:"dataBase,omitempty"`
	TableName            string            `protobuf:"bytes,2,opt,name=tableName,proto3" json:"tableName,omitempty"`
	Tags                 map[string]string `protobuf:"bytes,3,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Value                map[string]*Value `protobuf:"bytes,4,rep,name=value,proto3" json:"value,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *WritePoint) Reset()         { *m = WritePoint{} }
func (m *WritePoint) String() string { return proto.CompactTextString(m) }
func (*WritePoint) ProtoMessage()    {}
func (*WritePoint) Descriptor() ([]byte, []int) {
	return fileDescriptor_dbb1a16d5866e018, []int{0}
}

func (m *WritePoint) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_WritePoint.Unmarshal(m, b)
}
func (m *WritePoint) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_WritePoint.Marshal(b, m, deterministic)
}
func (m *WritePoint) XXX_Merge(src proto.Message) {
	xxx_messageInfo_WritePoint.Merge(m, src)
}
func (m *WritePoint) XXX_Size() int {
	return xxx_messageInfo_WritePoint.Size(m)
}
func (m *WritePoint) XXX_DiscardUnknown() {
	xxx_messageInfo_WritePoint.DiscardUnknown(m)
}

var xxx_messageInfo_WritePoint proto.InternalMessageInfo

func (m *WritePoint) GetDataBase() string {
	if m != nil {
		return m.DataBase
	}
	return ""
}

func (m *WritePoint) GetTableName() string {
	if m != nil {
		return m.TableName
	}
	return ""
}

func (m *WritePoint) GetTags() map[string]string {
	if m != nil {
		return m.Tags
	}
	return nil
}

func (m *WritePoint) GetValue() map[string]*Value {
	if m != nil {
		return m.Value
	}
	return nil
}

type Value struct {
	Kv                   map[int64]float64 `protobuf:"bytes,1,rep,name=kv,proto3" json:"kv,omitempty" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"fixed64,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *Value) Reset()         { *m = Value{} }
func (m *Value) String() string { return proto.CompactTextString(m) }
func (*Value) ProtoMessage()    {}
func (*Value) Descriptor() ([]byte, []int) {
	return fileDescriptor_dbb1a16d5866e018, []int{1}
}

func (m *Value) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Value.Unmarshal(m, b)
}
func (m *Value) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Value.Marshal(b, m, deterministic)
}
func (m *Value) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Value.Merge(m, src)
}
func (m *Value) XXX_Size() int {
	return xxx_messageInfo_Value.Size(m)
}
func (m *Value) XXX_DiscardUnknown() {
	xxx_messageInfo_Value.DiscardUnknown(m)
}

var xxx_messageInfo_Value proto.InternalMessageInfo

func (m *Value) GetKv() map[int64]float64 {
	if m != nil {
		return m.Kv
	}
	return nil
}

type ReadPoint struct {
	DataBase             string            `protobuf:"bytes,1,opt,name=dataBase,proto3" json:"dataBase,omitempty"`
	TableName            string            `protobuf:"bytes,2,opt,name=tableName,proto3" json:"tableName,omitempty"`
	Tags                 map[string]string `protobuf:"bytes,3,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Metrics              map[string]*Value `protobuf:"bytes,4,rep,name=metrics,proto3" json:"metrics,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	StartTime            int64             `protobuf:"varint,5,opt,name=startTime,proto3" json:"startTime,omitempty"`
	EndTime              int64             `protobuf:"varint,6,opt,name=endTime,proto3" json:"endTime,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *ReadPoint) Reset()         { *m = ReadPoint{} }
func (m *ReadPoint) String() string { return proto.CompactTextString(m) }
func (*ReadPoint) ProtoMessage()    {}
func (*ReadPoint) Descriptor() ([]byte, []int) {
	return fileDescriptor_dbb1a16d5866e018, []int{2}
}

func (m *ReadPoint) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ReadPoint.Unmarshal(m, b)
}
func (m *ReadPoint) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ReadPoint.Marshal(b, m, deterministic)
}
func (m *ReadPoint) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ReadPoint.Merge(m, src)
}
func (m *ReadPoint) XXX_Size() int {
	return xxx_messageInfo_ReadPoint.Size(m)
}
func (m *ReadPoint) XXX_DiscardUnknown() {
	xxx_messageInfo_ReadPoint.DiscardUnknown(m)
}

var xxx_messageInfo_ReadPoint proto.InternalMessageInfo

func (m *ReadPoint) GetDataBase() string {
	if m != nil {
		return m.DataBase
	}
	return ""
}

func (m *ReadPoint) GetTableName() string {
	if m != nil {
		return m.TableName
	}
	return ""
}

func (m *ReadPoint) GetTags() map[string]string {
	if m != nil {
		return m.Tags
	}
	return nil
}

func (m *ReadPoint) GetMetrics() map[string]*Value {
	if m != nil {
		return m.Metrics
	}
	return nil
}

func (m *ReadPoint) GetStartTime() int64 {
	if m != nil {
		return m.StartTime
	}
	return 0
}

func (m *ReadPoint) GetEndTime() int64 {
	if m != nil {
		return m.EndTime
	}
	return 0
}

func init() {
	proto.RegisterType((*WritePoint)(nil), "point.writePoint")
	proto.RegisterMapType((map[string]string)(nil), "point.writePoint.TagsEntry")
	proto.RegisterMapType((map[string]*Value)(nil), "point.writePoint.ValueEntry")
	proto.RegisterType((*Value)(nil), "point.value")
	proto.RegisterMapType((map[int64]float64)(nil), "point.value.KvEntry")
	proto.RegisterType((*ReadPoint)(nil), "point.readPoint")
	proto.RegisterMapType((map[string]*Value)(nil), "point.readPoint.MetricsEntry")
	proto.RegisterMapType((map[string]string)(nil), "point.readPoint.TagsEntry")
}

func init() { proto.RegisterFile("point.proto", fileDescriptor_dbb1a16d5866e018) }

var fileDescriptor_dbb1a16d5866e018 = []byte{
	// 324 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xa4, 0x53, 0x41, 0x4b, 0xf3, 0x40,
	0x14, 0x24, 0x9b, 0xa6, 0xf9, 0xf2, 0xd2, 0xc3, 0xc7, 0xd2, 0xc3, 0x12, 0x2b, 0x94, 0xe0, 0xa1,
	0xa7, 0x08, 0x11, 0xa9, 0x78, 0x14, 0x14, 0x41, 0x14, 0x29, 0xc5, 0xfb, 0xd6, 0x2c, 0x25, 0xa4,
	0x49, 0xca, 0x66, 0x8d, 0xf4, 0x3f, 0xf9, 0xc7, 0xfc, 0x17, 0x92, 0xdd, 0x4d, 0xb2, 0x92, 0x9e,
	0xea, 0x2d, 0xf3, 0xde, 0xcc, 0xbc, 0x09, 0xc3, 0x82, 0xbf, 0x2f, 0xd3, 0x42, 0x44, 0x7b, 0x5e,
	0x8a, 0x12, 0x3b, 0x12, 0x84, 0x5f, 0x08, 0xe0, 0x93, 0xa7, 0x82, 0xbd, 0x36, 0x10, 0x07, 0xf0,
	0x2f, 0xa1, 0x82, 0xde, 0xd1, 0x8a, 0x11, 0x6b, 0x6e, 0x2d, 0xbc, 0x55, 0x87, 0xf1, 0x0c, 0x3c,
	0x41, 0x37, 0x3b, 0xf6, 0x42, 0x73, 0x46, 0x90, 0x5c, 0xf6, 0x03, 0x7c, 0x09, 0x23, 0x41, 0xb7,
	0x15, 0xb1, 0xe7, 0xf6, 0xc2, 0x8f, 0xcf, 0x22, 0x75, 0xab, 0xb7, 0x8e, 0xd6, 0x74, 0x5b, 0xdd,
	0x17, 0x82, 0x1f, 0x56, 0x92, 0x88, 0x63, 0x70, 0x6a, 0xba, 0xfb, 0x60, 0x64, 0x24, 0x15, 0xb3,
	0xa1, 0xe2, 0xad, 0x59, 0x2b, 0x89, 0xa2, 0x06, 0x4b, 0xf0, 0x3a, 0x1b, 0xfc, 0x1f, 0xec, 0x8c,
	0x1d, 0x74, 0xcc, 0xe6, 0x13, 0x4f, 0x5b, 0x4b, 0x95, 0x4e, 0x81, 0x5b, 0x74, 0x63, 0x05, 0x0f,
	0x00, 0xbd, 0xdb, 0x11, 0x65, 0x68, 0x2a, 0xfd, 0x78, 0xa2, 0xc3, 0xc8, 0x99, 0xe1, 0x13, 0x26,
	0x9a, 0x87, 0x2f, 0x00, 0x65, 0x35, 0xb1, 0x64, 0xf4, 0xa9, 0xc9, 0x8e, 0x9e, 0x6a, 0x15, 0x19,
	0x65, 0x75, 0x70, 0x0d, 0xae, 0x86, 0xe6, 0x4d, 0xfb, 0x48, 0x5a, 0xcb, 0xbc, 0xf2, 0x8d, 0xc0,
	0xe3, 0x8c, 0x26, 0x7f, 0xed, 0x24, 0xfa, 0xd5, 0x49, 0xa0, 0x63, 0x76, 0xce, 0x83, 0x4a, 0x96,
	0xe0, 0xe6, 0x4c, 0xf0, 0xf4, 0xbd, 0xd2, 0xa5, 0x9c, 0x0f, 0x24, 0xcf, 0x6a, 0xaf, 0x54, 0x2d,
	0xbb, 0x89, 0x51, 0x09, 0xca, 0xc5, 0x3a, 0xcd, 0x19, 0x71, 0xe4, 0x2f, 0xf6, 0x03, 0x4c, 0xc0,
	0x65, 0x45, 0x22, 0x77, 0x63, 0xb9, 0x6b, 0xe1, 0xe9, 0x7d, 0x3e, 0xc2, 0xc4, 0x4c, 0x72, 0x7a,
	0xa3, 0x9b, 0xb1, 0x7c, 0x0e, 0x57, 0x3f, 0x01, 0x00, 0x00, 0xff, 0xff, 0xe2, 0xd2, 0x89, 0x99,
	0x1d, 0x03, 0x00, 0x00,
}
