package main

type ProtocolAction uint32

const (
	_                            ProtocolAction = iota
	ProtocolActionDocumentAdd                   = 1
	ProtocolActionDocumentDelete                = 2
	ProtocolActionDocumentLookup                = 3
)
