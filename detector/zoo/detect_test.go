package zoo

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/detector"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

const (
	zkurl     = "zk://127.0.0.1:2181/mesos"
	zkurl_bad = "zk://127.0.0.1:2181"
)

func TestParseZk_single(t *testing.T) {
	hosts, path, err := parseZk(zkurl)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(hosts))
	assert.Equal(t, "/mesos", path)
}

func TestParseZk_multi(t *testing.T) {
	hosts, path, err := parseZk("zk://abc:1,def:2/foo")
	assert.NoError(t, err)
	assert.Equal(t, []string{"abc:1", "def:2"}, hosts)
	assert.Equal(t, "/foo", path)
}

func TestParseZk_multiIP(t *testing.T) {
	hosts, path, err := parseZk("zk://10.186.175.156:2181,10.47.50.94:2181,10.0.92.171:2181/mesos")
	assert.NoError(t, err)
	assert.Equal(t, []string{"10.186.175.156:2181", "10.47.50.94:2181", "10.0.92.171:2181"}, hosts)
	assert.Equal(t, "/mesos", path)
}

func TestMasterDetectorStart(t *testing.T) {
	c, err := makeClient()
	assert.False(t, c.isConnected())
	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)
	c.errorHandler = ErrorHandler(func(c *Client, e error) {
		err = e
	})
	md.client = c // override zk.Conn with our own.
	md.client.connect()
	assert.NoError(t, err)
	assert.True(t, c.isConnected())
}

func TestMasterDetectorChildrenChanged(t *testing.T) {
	wCh := make(chan struct{}, 1)

	c, err := makeClient()
	assert.NoError(t, err)
	assert.False(t, c.isConnected())

	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)
	// override zk.Conn with our own.
	c.errorHandler = ErrorHandler(func(c *Client, e error) {
		err = e
	})
	md.client = c
	md.client.connect()
	assert.NoError(t, err)
	assert.True(t, c.isConnected())

	called := 0
	md.Detect(detector.OnMasterChanged(func(master *mesos.MasterInfo) {
		//expect 2 calls in sequence: the first setting a master
		//and the second clearing it
		switch called++; called {
		case 1:
			assert.NotNil(t, master)
			assert.Equal(t, master.GetId(), "master@localhost:5050")
			wCh <- struct{}{}
		case 2:
			assert.Nil(t, master)
			wCh <- struct{}{}
		default:
			t.Fatalf("unexpected notification call attempt %d", called)
		}
	}))

	startWait := time.Now()
	select {
	case <-wCh:
	case <-time.After(time.Second * 3):
		panic("Waited too long...")
	}

	// wait for the disconnect event, should be triggered
	// 1s after the connected event
	waited := time.Now().Sub(startWait)
	time.Sleep((2 * time.Second) - waited)
	assert.False(t, c.isConnected())
}

func TestMasterDetectMultiple(t *testing.T) {
	ch0 := make(chan zk.Event, 5)
	ch1 := make(chan zk.Event, 5)

	ch0 <- zk.Event{
		State: zk.StateConnected,
		Path:  test_zk_path,
	}

	c, err := newClient(test_zk_hosts, test_zk_path)
	assert.NoError(t, err)

	initialChildren := []string{"info_005", "info_010", "info_022"}
	connector := NewMockConnector()
	connector.On("Close").Return(nil)
	connector.On("Children", test_zk_path).Return(initialChildren, &zk.Stat{}, nil).Once()
	connector.On("ChildrenW", test_zk_path).Return([]string{test_zk_path}, &zk.Stat{}, (<-chan zk.Event)(ch1), nil)

	first := true
	c.setFactory(asFactory(func() (Connector, <-chan zk.Event, error) {
		log.V(2).Infof("**** Using zk.Conn adapter ****")
		if !first {
			return nil, nil, errors.New("only 1 connector allowed")
		} else {
			first = false
		}
		return connector, ch0, nil
	}))

	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)

	c.errorHandler = ErrorHandler(func(c *Client, e error) {
		err = e
	})
	md.client = c

	// **** Test 4 consecutive ChildrenChangedEvents ******
	// setup event changes
	sequences := [][]string{
		[]string{"info_014", "info_010", "info_005"},
		[]string{"info_005", "info_004", "info_022"},
		[]string{}, // indicates no master
		[]string{"info_017", "info_099", "info_200"},
	}

	var wg sync.WaitGroup
	startTime := time.Now()
	detected := 0
	md.Detect(detector.OnMasterChanged(func(master *mesos.MasterInfo) {
		if detected == 2 {
			assert.Nil(t, master, fmt.Sprintf("on-master-changed-%d", detected))
		} else {
			assert.NotNil(t, master, fmt.Sprintf("on-master-changed-%d", detected))
		}
		t.Logf("Leader change detected at %v: '%+v'", time.Now().Sub(startTime), master)
		detected++
		wg.Done()
	}))

	// 3 leadership changes + disconnect (leader change to '')
	wg.Add(4)

	go func() {
		for i := range sequences {
			sorted := make([]string, len(sequences[i]))
			copy(sorted, sequences[i])
			sort.Strings(sorted)
			t.Logf("testing master change sequence %d, path '%v'", i, test_zk_path)
			connector.On("Children", test_zk_path).Return(sequences[i], &zk.Stat{}, nil).Once()
			if len(sequences[i]) > 0 {
				connector.On("Get", fmt.Sprintf("%s/%s", test_zk_path, sorted[0])).Return(newTestMasterInfo(i), &zk.Stat{}, nil).Once()
			}
			ch1 <- zk.Event{
				Type: zk.EventNodeChildrenChanged,
				Path: test_zk_path,
			}
			time.Sleep(100 * time.Millisecond) // give async routines time to catch up
		}
		time.Sleep(1 * time.Second) // give async routines time to catch up
		t.Logf("disconnecting...")
		ch0 <- zk.Event{
			State: zk.StateDisconnected,
		}
		//TODO(jdef) does order of close matter here? probably, meaking client code is weak
		close(ch0)
		time.Sleep(500 * time.Millisecond) // give async routines time to catch up
		close(ch1)
	}()
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		wg.Wait()
	}()

	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()

	select {
	case <-time.After(2 * time.Second):
		panic("timed out waiting for master changes to propagate")
	case <-completed:
	}
}

func TestMasterDetect_selectTopNode_none(t *testing.T) {
	assert := assert.New(t)
	nodeList := []string{}
	node := selectTopNode(nodeList)
	assert.Equal("", node)
}

func TestMasterDetect_selectTopNode_0000x(t *testing.T) {
	assert := assert.New(t)
	nodeList := []string{
		"info_0000000046",
		"info_0000000032",
		"info_0000000058",
		"info_0000000061",
		"info_0000000008",
	}
	node := selectTopNode(nodeList)
	assert.Equal("info_0000000008", node)
}

func TestMasterDetect_selectTopNode_mixedEntries(t *testing.T) {
	assert := assert.New(t)
	nodeList := []string{
		"info_0000000046",
		"info_0000000032",
		"foo_lskdjfglsdkfsdfgdfg",
		"info_0000000061",
		"log_replicas_fdgwsdfgsdf",
		"bar",
	}
	node := selectTopNode(nodeList)
	assert.Equal("info_0000000032", node)
}
