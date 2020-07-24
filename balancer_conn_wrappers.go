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
	"fmt"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/internal/buffer"
	"google.golang.org/grpc/internal/channelz"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/resolver"
)

// scStateUpdate contains the subConn and the new state it changed to.
type scStateUpdate struct {
	sc    balancer.SubConn   // 子连接
	state connectivity.State // 连接状态
	err   error
}

// ccBalancerWrapper is a wrapper on top of cc for balancers.
// It implements balancer.ClientConn interface. 实现 balancer.ClientConn 接口
type ccBalancerWrapper struct {
	cc         *ClientConn
	balancerMu sync.Mutex // synchronizes calls to the balancer
	balancer   balancer.Balancer
	scBuffer   *buffer.Unbounded
	done       *grpcsync.Event

	mu       sync.Mutex
	subConns map[*acBalancerWrapper]struct{} // 子连接对象哈希表
}

func newCCBalancerWrapper(cc *ClientConn, b balancer.Builder, bopts balancer.BuildOptions) *ccBalancerWrapper {
	ccb := &ccBalancerWrapper{
		cc:       cc,
		scBuffer: buffer.NewUnbounded(),
		done:     grpcsync.NewEvent(),
		subConns: make(map[*acBalancerWrapper]struct{}),
	}

	// 独立协程
	go ccb.watcher()

	// 构建负载均衡器
	ccb.balancer = b.Build(ccb, bopts)

	return ccb
}

// watcher balancer functions sequentially, so the balancer can be implemented
// lock-free.
func (ccb *ccBalancerWrapper) watcher() {
	for {
		select {
		case t := <-ccb.scBuffer.Get():
			ccb.scBuffer.Load()
			if ccb.done.HasFired() {
				break
			}
			ccb.balancerMu.Lock()

			su := t.(*scStateUpdate)
			// 子连接状态更新事件处理：pickfirstBalancer->UpdateSubConnState
			ccb.balancer.UpdateSubConnState(su.sc, balancer.SubConnState{ConnectivityState: su.state, ConnectionError: su.err})

			ccb.balancerMu.Unlock()
		case <-ccb.done.Done():
		}

		if ccb.done.HasFired() {
			ccb.balancer.Close()
			ccb.mu.Lock()
			scs := ccb.subConns
			ccb.subConns = nil
			ccb.mu.Unlock()
			for acbw := range scs {
				ccb.cc.removeAddrConn(acbw.getAddrConn(), errConnDrain)
			}
			ccb.UpdateState(balancer.State{ConnectivityState: connectivity.Connecting, Picker: nil})
			return
		}
	}
}

func (ccb *ccBalancerWrapper) close() {
	ccb.done.Fire()
}

// 子连接状态变更通知
func (ccb *ccBalancerWrapper) handleSubConnStateChange(sc balancer.SubConn, s connectivity.State, err error) {
	// When updating addresses for a SubConn, if the address in use is not in
	// the new addresses, the old ac will be tearDown() and a new ac will be
	// created. tearDown() generates a state change with Shutdown state, we
	// don't want the balancer to receive this state change. So before
	// tearDown() on the old ac, ac.acbw (acWrapper) will be set to nil, and
	// this function will be called with (nil, Shutdown). We don't need to call
	// balancer method in this case.
	if sc == nil {
		return
	}
	ccb.scBuffer.Put(&scStateUpdate{
		sc:    sc,
		state: s,
		err:   err,
	})
}

// 更新 负载均衡器相关联的 连接状态
func (ccb *ccBalancerWrapper) updateClientConnState(ccs *balancer.ClientConnState) error {
	ccb.balancerMu.Lock()
	defer ccb.balancerMu.Unlock()
	return ccb.balancer.UpdateClientConnState(*ccs)
}

func (ccb *ccBalancerWrapper) resolverError(err error) {
	ccb.balancerMu.Lock()
	ccb.balancer.ResolverError(err)
	ccb.balancerMu.Unlock()
}

// 基于地址列表 创建新的子连接
func (ccb *ccBalancerWrapper) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	if len(addrs) <= 0 {
		return nil, fmt.Errorf("grpc: cannot create SubConn with empty address list")
	}

	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	// 检查连接是否已经关闭
	if ccb.subConns == nil {
		return nil, fmt.Errorf("grpc: ClientConn balancer wrapper was closed")
	}

	// 基于地址列表 创建网络连接对象
	ac, err := ccb.cc.newAddrConn(addrs, opts)
	if err != nil {
		return nil, err
	}

	acbw := &acBalancerWrapper{ac: ac}
	acbw.ac.mu.Lock()
	ac.acbw = acbw
	acbw.ac.mu.Unlock()
	ccb.subConns[acbw] = struct{}{}
	return acbw, nil
}

func (ccb *ccBalancerWrapper) RemoveSubConn(sc balancer.SubConn) {
	acbw, ok := sc.(*acBalancerWrapper)
	if !ok {
		return
	}
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	if ccb.subConns == nil {
		return
	}
	delete(ccb.subConns, acbw)
	ccb.cc.removeAddrConn(acbw.getAddrConn(), errConnDrain)
}

// 状态更新处理
func (ccb *ccBalancerWrapper) UpdateState(s balancer.State) {
	ccb.mu.Lock()
	defer ccb.mu.Unlock()
	if ccb.subConns == nil {
		return
	}
	// Update picker before updating state.  Even though the ordering here does
	// not matter, it can lead to multiple calls of Pick in the common start-up
	// case where we wait for ready and then perform an RPC.  If the picker is
	// updated later, we could call the "connecting" picker when the state is
	// updated, and then call the "ready" picker after the picker gets updated.
	ccb.cc.blockingpicker.updatePicker(s.Picker)

	// 逻辑连接的状态更新
	ccb.cc.csMgr.updateState(s.ConnectivityState)
}

func (ccb *ccBalancerWrapper) ResolveNow(o resolver.ResolveNowOptions) {
	ccb.cc.resolveNow(o)
}

func (ccb *ccBalancerWrapper) Target() string {
	return ccb.cc.target
}

// acBalancerWrapper is a wrapper on top of ac for balancers.
// It implements balancer.SubConn interface. 实现 balancer.SubConn 接口
type acBalancerWrapper struct {
	mu sync.Mutex
	ac *addrConn // 基于地址列表的网络连接对象
}

// 更新地址列表
func (acbw *acBalancerWrapper) UpdateAddresses(addrs []resolver.Address) {
	acbw.mu.Lock()
	defer acbw.mu.Unlock()
	if len(addrs) <= 0 {
		// 关闭现有的所有网络连接
		acbw.ac.tearDown(errConnDrain)
		return
	}

	// 尝试更新网络连接地址列表
	if acbw.ac.tryUpdateAddrs(addrs) == false {
		cc := acbw.ac.cc
		opts := acbw.ac.scopts
		acbw.ac.mu.Lock()
		// Set old ac.acbw to nil so the Shutdown state update will be ignored
		// by balancer.
		//
		// TODO(bar) the state transition could be wrong when tearDown() old ac
		// and creating new ac, fix the transition.
		acbw.ac.acbw = nil
		acbw.ac.mu.Unlock()
		acState := acbw.ac.getState()
		// 优雅关闭当前的网络连接
		acbw.ac.tearDown(errConnDrain)

		if acState == connectivity.Shutdown {
			return
		}

		// 尝试基于当前的地址列表，重新初始化连接
		ac, err := cc.newAddrConn(addrs, opts)
		if err != nil {
			channelz.Warningf(acbw.ac.channelzID, "acBalancerWrapper: UpdateAddresses: failed to newAddrConn: %v", err)
			return
		}
		acbw.ac = ac
		ac.mu.Lock()
		ac.acbw = acbw
		ac.mu.Unlock()
		if acState != connectivity.Idle {
			ac.connect()
		}
	}
}

// 网络连接对象 连接服务端
func (acbw *acBalancerWrapper) Connect() {
	acbw.mu.Lock()
	defer acbw.mu.Unlock()
	acbw.ac.connect()
}

func (acbw *acBalancerWrapper) getAddrConn() *addrConn {
	acbw.mu.Lock()
	defer acbw.mu.Unlock()
	return acbw.ac
}
