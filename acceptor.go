package paxos

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

type Acceptor struct {
	lis net.Listener //用于启动RPC服务并启用监听端口
	//服务器id
	id int
	//接受者承诺的提案编号，若为0，表明接受者没有承诺任何的prepare消息
	minProposal int
	//接受者接受的提案编号，如果为0表示没有接受任何提案
	acceptedNumber int
	//接受者已经接受的提案值，如果没有接受任何的提案，则为nil
	acceptedValue interface{}

	//学习者id列表 用于存储学习者的端口 RPC相关的变量
	learners []int
}

// 第一阶段的处理函数
func (a *Acceptor) Prepare(args *MsgArgs, reply *MsgReply) error {
	if args.Number > a.minProposal { //发来的提案编号大于见过的最大提案编号，承诺不会接收编号小于args.number的提案
		a.minProposal = args.Number
		reply.Number = a.acceptedNumber
		reply.Value = a.acceptedValue
		reply.Ok = true
	} else {
		reply.Ok = false
	}
	return nil
}

func (a *Acceptor) Accept(args *MsgArgs, reply *MsgReply) error {
	if args.Number >= a.minProposal { //在此期间没有承诺比这个提案编号更大的提案，接受提案并转发到所有学习者
		a.minProposal = args.Number
		a.acceptedNumber = args.Number
		a.acceptedValue = args.Value
		reply.Ok = true
		//后台转发接受的提案到学习者
		for _, lid := range a.learners {
			go func(learner int) {
				addr := fmt.Sprintf("127.0.0.1:%d", learner)
				args.From = a.id
				args.To = learner
				resp := new(MsgReply)
				ok := call(addr, "Learner.Learn", args, resp)
				if !ok {
					return
				}
			}(lid)
		}
	} else {
		reply.Ok = false
	}
	return nil
}

// 实现与RPC服务相关的逻辑，以及初始化接受者的函数
func newAcceptor(id int, learners []int) *Acceptor {
	acceptor := &Acceptor{
		id:       id,
		learners: learners,
	}
	acceptor.server()
	return acceptor
}

func (a *Acceptor) server() {
	rpcs := rpc.NewServer()
	rpcs.Register(a)
	addr := fmt.Sprintf(":%d", a.id)
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	a.lis = l
	go func() {
		for {
			conn, err := a.lis.Accept()
			if err != nil {
				continue
			}
			go rpcs.ServeConn(conn)
		}
	}()
}

// 关闭连接
func (a *Acceptor) close() {
	a.lis.Close()
}
