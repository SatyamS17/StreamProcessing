package rainstorm

import (
	"net/rpc"
)

func rpcCall(address string, function string, args any) error {
	client, err := rpc.DialHTTP("tcp", address+":"+rpcPortNumber)
	if err != nil {
		return err
	}

	var reply struct{}
	err = client.Call(function, args, &reply)

	// TODO retry if timeout
	if err != nil {
		return err
	}
	return nil
}
