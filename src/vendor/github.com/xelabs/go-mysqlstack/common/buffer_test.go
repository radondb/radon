/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestBuffer(t *testing.T) {
	writer := NewBuffer(6)
	writer.WriteU32(22222232)
	writer.WriteU32(31)
	writer.WriteU32(30)
	writer.WriteU8(208)
	writer.WriteU16(65535)
	writer.WriteBytes([]byte{1, 2, 3, 4, 5})
	writer.WriteZero(3)
	writer.WriteString("abc")
	writer.WriteEOF(1)
	writer.WriteString("xyz")
	writer.WriteEOF(2)
	writer.WriteU24(1024)

	{
		want := (4 + 4 + 4 + 1 + 2 + 5 + 3 + 3 + 1 + 3 + 2 + 3)
		got := writer.Length()
		assert.Equal(t, want, got)
	}

	{
		want := uint32(22222232)
		got, _ := writer.ReadU32()
		assert.Equal(t, want, got)
	}

	{
		want := uint32(31)
		got, _ := writer.ReadU32()
		assert.Equal(t, want, got)
	}

	{
		want := uint32(30)
		got, _ := writer.ReadU32()
		assert.Equal(t, want, got)
	}

	{
		want := uint8(208)
		got, _ := writer.ReadU8()
		assert.Equal(t, want, got)
	}

	{
		want := uint16(65535)
		got, _ := writer.ReadU16()
		assert.Equal(t, want, got)
	}

	{
		want := []byte{1, 2, 3, 4, 5}
		got, _ := writer.ReadBytes(5)
		assert.Equal(t, want, got)
	}

	{
		writer.ReadZero(3)
	}

	{
		want := "abc"
		got, _ := writer.ReadString(3)
		assert.Equal(t, want, got)
	}

	{
		writer.ReadEOF(1)
	}

	{
		want := "xyz"
		got, _ := writer.ReadStringEOF()
		assert.Equal(t, want, got)
	}

	{
		writer.ReadEOF(1)
	}

	{
		want := uint32(1024)
		got, _ := writer.ReadU24()
		assert.Equal(t, want, got)
	}

	{
		want := writer.Length()
		got := writer.Seek()
		assert.Equal(t, want, got)
	}

}

func TestBufferDatas(t *testing.T) {
	writer := NewBuffer(100)
	writer.WriteU32(22222232)
	writer.WriteString("abc")
	writer.WriteZero(2)

	{
		want := len(writer.Datas())
		got := writer.Length()
		assert.Equal(t, want, got)
	}

	{
		want := []byte{152, 21, 83, 1, 97, 98, 99, 0, 0}
		got := writer.Datas()
		assert.Equal(t, want, got)
	}
}

func TestBufferRead(t *testing.T) {
	data := []byte{152, 21, 83, 1, 97, 98, 99, 0, 0}
	writer := ReadBuffer(data)
	{
		want := uint32(22222232)
		got, _ := writer.ReadU32()
		assert.Equal(t, want, got)
	}

	{
		want := "abc"
		got, _ := writer.ReadString(3)
		assert.Equal(t, want, got)
	}
}

func TestBufferReadError(t *testing.T) {
	{
		data := []byte{152}
		writer := ReadBuffer(data)
		_, err := writer.ReadU8()
		assert.Nil(t, err)
	}

	{
		data := []byte{152}
		writer := ReadBuffer(data)
		want := io.EOF
		_, got := writer.ReadU16()
		assert.Equal(t, want.Error(), got.Error())
	}

	{
		data := []byte{152, 154}
		writer := ReadBuffer(data)
		want := io.EOF
		_, got := writer.ReadU24()
		assert.Equal(t, want.Error(), got.Error())
	}

	{
		data := []byte{152, 154, 155}
		writer := ReadBuffer(data)
		want := io.EOF
		_, got := writer.ReadU32()
		assert.Equal(t, want.Error(), got.Error())
	}

	{
		data := []byte{152, 154, 155}
		writer := ReadBuffer(data)
		want := io.EOF
		got := writer.ReadZero(4)
		assert.Equal(t, want.Error(), got.Error())
	}

	{
		data := []byte{152, 154, 155}
		writer := ReadBuffer(data)
		want := io.EOF
		_, got := writer.ReadString(4)
		assert.Equal(t, want.Error(), got.Error())
	}

	{
		data := []byte{152, 154, 155}
		writer := ReadBuffer(data)
		want := io.EOF
		_, got := writer.ReadStringNUL()
		assert.Equal(t, want.Error(), got.Error())
	}

	{
		data := []byte{152, 154, 155}
		writer := ReadBuffer(data)
		want := io.EOF
		_, got := writer.ReadBytes(4)
		assert.Equal(t, want.Error(), got.Error())
	}
}

func TestBufferReadString(t *testing.T) {
	data := []byte{
		0x98, 0x15, 0x53, 0x01, 0x61, 0x62, 0x63, 0xff,
		0xff, 0x61, 0x62, 0x63, 0x00, 0x00, 0xff, 0xff}
	writer := ReadBuffer(data)

	{
		want := 0
		got := writer.seek
		assert.Equal(t, want, got)
	}

	{
		want := 16
		got := writer.pos
		assert.Equal(t, want, got)
	}

	{
		want := 16
		got := writer.pos
		assert.Equal(t, want, got)
	}

	{
		want := uint32(22222232)
		got, _ := writer.ReadU32()
		assert.Equal(t, want, got)
	}

	{
		want := "abc"
		got, _ := writer.ReadString(3)
		assert.Equal(t, want, got)
	}

	{
		want := uint16(65535)
		got, _ := writer.ReadU16()
		assert.Equal(t, want, got)
	}

	{
		want := "abc"
		got, _ := writer.ReadStringNUL()
		assert.Equal(t, want, got)
	}

	{
		want := 13
		got := writer.seek
		assert.Equal(t, want, got)
		writer.ReadZero(1)
	}

	// here, we inject a ReadStringWithNUL
	// we never got it since here is ReadU16()
	{

		want := "EOF"
		_, err := writer.ReadStringNUL()
		got := err.Error()
		assert.Equal(t, want, got)
	}

	{
		want := 16
		got := writer.seek
		assert.Equal(t, want, got)
	}
}

func TestBufferLenEncode(t *testing.T) {
	writer := NewBuffer(6)

	{
		v := uint64(250)
		writer.WriteLenEncode(v)
	}

	{
		v := uint64(252)
		writer.WriteLenEncode(v)
	}

	{
		v := uint64(1 << 16)
		writer.WriteLenEncode(v)
	}

	{
		writer.WriteLenEncodeNUL()
	}

	{
		v := uint64(1 << 24)
		writer.WriteLenEncode(v)
	}

	{
		v := uint64(1<<24 + 1)
		writer.WriteLenEncode(v)
	}

	{
		v := uint64(0)
		writer.WriteLenEncode(v)
	}

	read := ReadBuffer(writer.Datas())

	{
		v, err := read.ReadLenEncode()
		assert.Nil(t, err)
		assert.Equal(t, v, uint64(250))
	}

	{
		v, err := read.ReadLenEncode()
		assert.Nil(t, err)
		assert.Equal(t, v, uint64(252))
	}

	{
		v, err := read.ReadLenEncode()
		assert.Nil(t, err)
		assert.Equal(t, v, uint64(1<<16))
	}

	{
		v, err := read.ReadLenEncode()
		assert.Nil(t, err)
		assert.Equal(t, v, ^uint64(0))
	}

	{
		v, err := read.ReadLenEncode()
		assert.Nil(t, err)
		assert.Equal(t, v, uint64(1<<24))
	}

	{
		v, err := read.ReadLenEncode()
		assert.Nil(t, err)
		assert.Equal(t, v, uint64(1<<24+1))
	}
	{
		v, err := read.ReadLenEncode()
		assert.Nil(t, err)
		assert.Equal(t, v, uint64(0))
	}

}

func TestBufferLenEncodeString(t *testing.T) {
	writer := NewBuffer(6)
	reader := NewBuffer(6)

	s1 := "BohuTANG"
	b1 := []byte{0x01, 0x02}
	b11 := []byte{}
	b12 := []byte(nil)
	{
		v := uint64(len(s1))
		writer.WriteLenEncode(v)
		writer.WriteString(s1)
		writer.WriteLenEncodeString(s1)
		writer.WriteLenEncodeBytes(b1)
		writer.WriteLenEncodeNUL()
		writer.WriteLenEncodeBytes(b11)
		reader.Reset(writer.Datas())
	}

	{
		got, err := reader.ReadLenEncodeString()
		assert.Nil(t, err)
		assert.Equal(t, s1, got)
	}

	{
		got, err := reader.ReadLenEncodeString()
		assert.Nil(t, err)
		assert.Equal(t, s1, got)
	}

	{
		got, err := reader.ReadLenEncodeBytes()
		assert.Nil(t, err)
		assert.Equal(t, b1, got)
	}

	{
		got, err := reader.ReadLenEncodeBytes()
		assert.Nil(t, err)
		assert.Equal(t, b12, got)
	}

	{
		got, err := reader.ReadLenEncodeBytes()
		assert.Nil(t, err)
		assert.Equal(t, b11, got)
	}
}

func TestBufferNULEOF(t *testing.T) {
	writer := NewBuffer(16)
	data1 := "BohuTANG"
	data2 := "radon"

	{
		writer.WriteString(data1)
		writer.WriteZero(1)
	}

	{
		writer.WriteString(data2)
		writer.WriteZero(1)
	}

	{
		writer.WriteString(data1)
		writer.WriteEOF(1)
	}

	{
		writer.WriteString(data2)
		writer.WriteEOF(1)
	}

	reader := ReadBuffer(writer.Datas())
	{
		got, _ := reader.ReadStringNUL()
		assert.Equal(t, data1, got)
	}

	{
		got, _ := reader.ReadBytesNUL()
		assert.Equal(t, StringToBytes(data2), got)
	}

	{
		got, _ := reader.ReadStringEOF()
		assert.Equal(t, data1, got)
	}

	{
		got, _ := reader.ReadBytesEOF()
		assert.Equal(t, StringToBytes(data2), got)
	}
}

func TestBufferReset(t *testing.T) {
	writer := NewBuffer(6)
	writer.WriteU32(31)
	writer.WriteU32(30)

	{
		want := uint32(31)
		got, _ := writer.ReadU32()
		assert.Equal(t, want, got)
		assert.Equal(t, writer.seek, 4)
	}

	{
		data := []byte{0x00, 0x00, 0x00, 0x01}
		writer.Reset(data)
		assert.Equal(t, writer.pos, 4)
		assert.Equal(t, writer.seek, 0)
	}
}

func TestBufferNUL(t *testing.T) {
	writer := NewBuffer(6)

	{
		writer.WriteLenEncodeNUL()
		got, _ := writer.ReadLenEncodeBytes()
		assert.Nil(t, got)
	}
}

func TestWriteBytesNil(t *testing.T) {
	writer := NewBuffer(6)

	{
		writer.WriteBytes(nil)
		reader := ReadBuffer(writer.Datas())
		got, _ := reader.ReadBytes(0)
		assert.Nil(t, got)
	}
}
