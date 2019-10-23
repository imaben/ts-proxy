package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
	"ts-proxy/protocol"

	"encoding/binary"

	"github.com/golang/protobuf/proto"
	"github.com/huichen/sego"
	"github.com/sirupsen/logrus"
)

const QueryDefaultLimit = 50
const QueryMaxinumLimit = 3500

type Shard struct {
	address string
}

type Searcher struct {
	Shards    []*Shard
	segmenter sego.Segmenter
}

type ShardResponse struct {
	code  int
	total uint32
	docs  []*ResponseDoc
}

type ShardQuery struct {
	request  *RequestQuery
	response chan *ShardResponse
}

var searcher *Searcher

func initSearcherProxy() {
	searcher = &Searcher{}
	for _, host := range ConfigSearcherList {
		searcher.Shards = append(searcher.Shards, &Shard{address: host})
	}
	searcher.segmenter.LoadDictionary("data/dictionary.txt")
}

func (s *Searcher) getShard(idx uint64) *Shard {
	return s.Shards[idx%uint64(len(s.Shards))]
}

func (s *Searcher) Index(idx *RequestIndex) int {
	return s.getShard(idx.ID).Index(idx)
}

func (s *Searcher) Delete(del *RequestDelete) int {
	return s.getShard(del.ID).Delete(del.ID)
}

func (s *Searcher) Query(query *RequestQuery) (int, *ResponseQuery) {
	if query.Limit == 0 {
		query.Limit = QueryDefaultLimit
	}
	if query.Limit+query.Offset > QueryMaxinumLimit {
		return CodeExceedLimit, nil
	}
	// 查询需要向所有的分片同时发起查询请求
	responseChans := make([]chan *ShardResponse, len(s.Shards))
	for k, shard := range s.Shards {
		shardQuery := &ShardQuery{
			request: query,
		}
		shardQuery.response = make(chan *ShardResponse)
		responseChans[k] = shardQuery.response
		go shard.Query(shardQuery)
	}

	logrus.Warnf("shard len:%d, respchans len:%d", len(s.Shards), len(responseChans))
	isTimeout := false
	result := &ResponseQuery{}
	deadline := time.Now().Add(time.Second * 1) // timeout: 1s
	for key, ch := range responseChans {
		logrus.Warnf("search from shard %d", key)
		select {
		case response := <-ch:
			if response.code != CodeSuccess {
				logrus.Errorf("query shard %d fail, code:%d", key, response.code)
				continue
			}
			result.Total += response.total
			for _, doc := range response.docs {
				result.Docs = append(result.Docs, doc)
			}
			continue
		case <-time.After(deadline.Sub(time.Now())):
			isTimeout = true
			break
		}
	}
	if isTimeout {
		return CodeTimeout, nil
	}
	// 假装此处有排序
	// sortByRank
	limit := int(query.Limit)
	if limit > len(result.Docs) {
		limit = len(result.Docs)
	}
	result.Docs = result.Docs[0:limit]
	return CodeSuccess, result
}

func (s *Shard) Index(idx *RequestIndex) int {
	request := &protocol.RequestDocumentAdd{}
	document := &protocol.Document{
		Pk: idx.ID,
	}

	for k, v := range idx.Properties {
		field := &protocol.Field{
			Type:      0,
			Term:      k,
			ValueStr:  v.Content,
			Indexable: false,
		}
		document.Fields = append(document.Fields, field)

		if v.SegmentWord {
			segs := searcher.segmenter.Segment([]byte(v.Content))
			if len(segs) > 0 {
				for _, seg := range segs {
					if len(seg.Token().Text()) == 0 {
						continue
					}
					term := fmt.Sprintf("_%s_%s", k, seg.Token().Text())
					field := &protocol.Field{
						Type:       1,
						Term:       term,
						ValueInt32: 1,
						Indexable:  true,
					}
					document.Fields = append(document.Fields, field)
				}
			}
		} else {
			field = &protocol.Field{
				Type:       1,
				Term:       fmt.Sprintf("_%s_%s", k, v.Content),
				ValueInt32: 1,
				Indexable:  true,
			}
			document.Fields = append(document.Fields, field)
		}
	}
	request.Documents = append(request.Documents, document)
	data, err := proto.Marshal(request)
	if err != nil {
		logrus.Errorf("proto encode error[%s]", err.Error())
		return CodeInternalError
	}
	recvData, err := s.request(ProtocolActionDocumentAdd, data)
	if err != nil {
		return CodeInternalError
	}
	response := &protocol.ResponseOperate{}
	if err := proto.Unmarshal(recvData, response); err != nil {
		logrus.Errorf("protobuf unmarshall fail, :%s", err.Error())
		return CodeInternalError
	}
	if response.Code != protocol.ErrorCode_ERROR_CODE_SUCCESS {
		logrus.Errorf("add document fail:%s", response.Msg)
		return CodeInternalError
	}
	return CodeSuccess
}

func (s *Shard) Delete(id uint64) int {
	request := &protocol.RequestDocumentRemove{}
	request.Pks = append(request.Pks, id)
	data, err := proto.Marshal(request)
	if err != nil {
		logrus.Errorf("proto encode error[%s]", err.Error())
		return CodeInternalError
	}
	recvData, err := s.request(ProtocolActionDocumentDelete, data)
	if err != nil {
		return CodeInternalError
	}
	response := &protocol.ResponseOperate{}
	if err := proto.Unmarshal(recvData, response); err != nil {
		logrus.Error(recvData)
		logrus.Errorf("protobuf unmarshall fail, :%s", err.Error())
		return CodeInternalError
	}
	if response.Code != protocol.ErrorCode_ERROR_CODE_SUCCESS {
		logrus.Errorf("del document fail:%s", response.Msg)
		return CodeInternalError
	}
	return CodeSuccess
}

func (s *Shard) Query(query *ShardQuery) {
	request := &protocol.RequestLookup{}
	shardResponse := &ShardResponse{}
	for k, v := range query.request.Properties {
		if v.SegmentWord {
			logrus.Warnf("query:%s", v.Content)
			segs := searcher.segmenter.Segment([]byte(v.Content))
			if len(segs) > 0 {
				for _, seg := range segs {
					term := fmt.Sprintf("_%s_%s", k, seg.Token().Text())
					request.Terms = append(request.Terms, term)
				}
			}
		} else {
			term := fmt.Sprintf("_%s_%s", k, v.Content)
			request.Terms = append(request.Terms, term)
		}
	}
	request.Limit = query.request.Offset + query.request.Limit
	fmt.Println(request.Terms)
	data, err := proto.Marshal(request)
	if err != nil {
		logrus.Errorf("proto encode error[%s]", err.Error())
		shardResponse.code = CodeInternalError
		query.response <- shardResponse
		return
	}
	recvData, err := s.request(ProtocolActionDocumentLookup, data)
	if err != nil {
		logrus.Errorf("request shard fail[%s]", err.Error())
		shardResponse.code = CodeInternalError
		query.response <- shardResponse
		return
	}
	response := &protocol.ResponseLookup{}
	if err := proto.Unmarshal(recvData, response); err != nil {
		logrus.Errorf("protobuf unmarshall fail, recvData len:%d, msg:%s", len(recvData), err.Error())
		shardResponse.code = CodeInternalError
		query.response <- shardResponse
		return
	}
	if response.Code != protocol.ErrorCode_ERROR_CODE_SUCCESS {
		logrus.Errorf("query document fail:%s", response.Msg)
		shardResponse.code = CodeInternalError
		query.response <- shardResponse
		return
	}
	shardResponse.code = CodeSuccess
	shardResponse.total = response.Total
	if len(response.Documents) > int(query.request.Offset) {
		for _, doc := range response.Documents[query.request.Offset:] {
			rdoc := &ResponseDoc{}
			rdoc.ID = doc.GetPk()
			rdoc.Properties = make(map[string]interface{})
			for _, field := range doc.GetFields() {
				if field.GetIndexable() {
					continue
				}
				if field.GetType() == 0 {
					rdoc.Properties[field.GetTerm()] = field.GetValueStr()
				} else {
					rdoc.Properties[field.GetTerm()] = field.GetValueInt32()
				}
			}
			shardResponse.docs = append(shardResponse.docs, rdoc)
		}
	}
	query.response <- shardResponse
}

func (s *Shard) request(action ProtocolAction, buf []byte) ([]byte, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", s.address)
	if err != nil {
		logrus.Errorf("ResolveTCPAddr fail, addr:%s, err:%s", s.address, err.Error())
		return nil, err
	}
	tcpConn, err := net.DialTCP("tcp4", nil, tcpAddr)
	if err != nil {
		logrus.Errorf("connect to %s fail, err:%s", s.address, err.Error())
		return nil, err
	}
	defer tcpConn.Close()
	buflen := uint32(len(buf) + 4)
	newbuf := make([]byte, len(buf)+8)
	binary.LittleEndian.PutUint32(newbuf[0:], buflen)
	binary.LittleEndian.PutUint32(newbuf[4:], uint32(action))
	copy(newbuf[8:], buf)

	// send
	n, err := tcpConn.Write(newbuf)
	if err != nil {
		logrus.Errorf("send data to %s fail, err:%s", s.address, err.Error())
		return nil, err
	}
	logrus.Debugf("sent %d byte to searcher[%s]", n, s.address)

	recvHeader := make([]byte, 4)
	rd := bufio.NewReader(tcpConn)
	n, err = rd.Read(recvHeader)
	if err != nil {
		logrus.Errorf("recv data from %s fail, err:%s", s.address, err.Error())
		return nil, err
	}
	recvLen := binary.LittleEndian.Uint32(recvHeader)
	recvData := make([]byte, recvLen)
	offset := 0
	for offset < int(recvLen) {
		n, err := rd.Read(recvData[offset:])
		if err != nil {
			logrus.Errorf("recv data from %s fail, err:%s", s.address, err.Error())
			return nil, err
		}
		offset += n
	}
	return recvData, nil
}
