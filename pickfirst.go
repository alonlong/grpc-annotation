/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package grpc

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/grpclog"
)

// PickFirstBalancerName is the name of the pick_first balancer.
const PickFirstBalancerName = "pick_first"

func newPickfirstBuilder() balancer.Builder {
	return &pickfirstBuilder{}
}

type pickfirstBuilder struct{}

func (*pickfirstBuilder) Build(cc balancer.ClientConn, opt balancer.BuildOptions) balancer.Balancer {
	return &pickfirstBalancer{cc: cc}
}

func (*pickfirstBuilder) Name() string {
	return PickFirstBalancerName
}

type pickfirstBalancer struct {
	state connectivity.State
	cc    balancer.ClientConn // 客户端连接
	sc    balancer.SubConn    // 子连接
}

func (b *pickfirstBalancer) ResolverError(err error) {
	switch b.state {
	case connectivity.TransientFailure, connectivity.Idle, connectivity.Connecting:
		// Set a failing picker if we don't have a good picker.
		b.cc.UpdateState(balancer.State{ConnectivityState: connectivity.TransientFailure,
			Picker: &picker{err: fmt.Errorf("name resolver error: %v", err)},
		})
	}
	if grpclog.V(2) {
		grpclog.Infof("pickfirstBalancer: ResolverError called with error %v", err)
	}
}

// UpdateClientConnState 负载均衡器相关联的 连接状态 更新事件处理
func (b *pickfirstBalancer) UpdateClientConnState(cs balancer.ClientConnState) error {
	// 地址列表为空
	if len(cs.ResolverState.Addresses) == 0 {
		b.ResolverError(errors.New("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	// 初始化子连接
	if b.sc == nil {
		grpclog.Infof("first time to NewSubConn: %+v", cs.ResolverState.Addresses)

		var err error
		// 基于地址列表初始化子连接，忽略子连接选项 -> ccBalancerWrapper
		b.sc, err = b.cc.NewSubConn(cs.ResolverState.Addresses, balancer.NewSubConnOptions{})
		if err != nil {
			if grpclog.V(2) {
				grpclog.Errorf("pickfirstBalancer: failed to NewSubConn: %v", err)
			}
			b.state = connectivity.TransientFailure
			b.cc.UpdateState(balancer.State{ConnectivityState: connectivity.TransientFailure,
				Picker: &picker{err: fmt.Errorf("error creating connection: %v", err)},
			})
			return balancer.ErrBadResolverState
		}

		// 更新连接状态：空闲状态
		b.state = connectivity.Idle
		b.cc.UpdateState(balancer.State{ConnectivityState: connectivity.Idle, Picker: &picker{result: balancer.PickResult{SubConn: b.sc}}})

		// 创建连接
		b.sc.Connect()
	} else {
		// 更新子连接地址列表
		b.sc.UpdateAddresses(cs.ResolverState.Addresses)

		// 创建连接
		b.sc.Connect()
	}
	return nil
}

// UpdateSubConnState 子连接状态更新事件处理
func (b *pickfirstBalancer) UpdateSubConnState(sc balancer.SubConn, s balancer.SubConnState) {
	if grpclog.V(2) {
		grpclog.Infof("pickfirstBalancer: UpdateSubConnState: %p, %v", sc, s)
	}
	if b.sc != sc {
		if grpclog.V(2) {
			grpclog.Infof("pickfirstBalancer: ignored state change because sc is not recognized")
		}
		return
	}

	// 子连接状态
	b.state = s.ConnectivityState
	if s.ConnectivityState == connectivity.Shutdown {
		b.sc = nil
		return
	}

	switch s.ConnectivityState {
	case connectivity.Ready, connectivity.Idle:
		// ccBalancerWrapper->UpdateState
		b.cc.UpdateState(balancer.State{ConnectivityState: s.ConnectivityState, Picker: &picker{result: balancer.PickResult{SubConn: sc}}})
	case connectivity.Connecting:
		b.cc.UpdateState(balancer.State{ConnectivityState: s.ConnectivityState, Picker: &picker{err: balancer.ErrNoSubConnAvailable}})
	case connectivity.TransientFailure:
		b.cc.UpdateState(balancer.State{ConnectivityState: s.ConnectivityState, Picker: &picker{err: s.ConnectionError}})
	}
}

func (b *pickfirstBalancer) Close() {
}

type picker struct {
	result balancer.PickResult
	err    error
}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	return p.result, p.err
}

func init() {
	balancer.Register(newPickfirstBuilder())
}
