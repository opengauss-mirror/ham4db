/*
	Copyright 2021 SANGFOR TECHNOLOGIES

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package consul

import (
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"net/http"
	"sync"
	"testing"

	"gitee.com/opengauss/ham4db/go/config"
	consulapi "github.com/hashicorp/consul/api"
)

func TestConsulTxnStorePutKVPairs(t *testing.T) {
	server := buildConsulTestServer(t, []consulTestServerOp{
		{
			Method:   "PUT",
			URL:      "/v1/kv/kv1",
			Request:  "test",
			Response: &consulapi.KVPair{Key: "kv1", Value: []byte("test")},
		},
		{
			Method: "PUT",
			URL:    "/v1/txn",
			Request: consulapi.TxnOps{
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVSet, Key: "kv1", Value: []byte("test")},
				},
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVSet, Key: "kv2", Value: []byte("test2")},
				},
			},
			Response: &consulapi.TxnResponse{
				Results: consulapi.TxnResults{
					{
						KV: &consulapi.KVPair{Key: "kv1", Value: []byte("test")},
					},
					{
						KV: &consulapi.KVPair{Key: "kv2", Value: []byte("test2")},
					},
				},
			},
		},
		{
			Method: "PUT",
			URL:    "/v1/txn",
			Request: consulapi.TxnOps{
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVSet, Key: "fail1", Value: []byte("test")},
				},
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVSet, Key: "fail2", Value: []byte("test2")},
				},
			},
			Response: &consulapi.TxnResponse{
				Errors: consulapi.TxnErrors{
					{
						What: "test error",
					},
				},
			},
			ResponseCode: http.StatusConflict, // PUT /v1/txn returns a HTTP 409 code on txn failure
		},
	})
	defer server.Close()
	config.Config.ConsulAddress = server.URL
	store := NewConsulTxnStore()

	t.Run("single-kv", func(t *testing.T) {
		if err := store.PutKVPairs([]*dtstruct.KVPair{
			{Key: "kv1", Value: "test"},
		}); err != nil {
			t.Fatalf("Unable to run .PutKVPairs(): %v", err)
		}
	})

	t.Run("multi-kv", func(t *testing.T) {
		if err := store.PutKVPairs([]*dtstruct.KVPair{
			{Key: "kv1", Value: "test"},
			{Key: "kv2", Value: "test2"},
		}); err != nil {
			t.Fatalf("Unable to run .PutKVPairs(): %v", err)
		}
	})

	t.Run("multi-kv-failed", func(t *testing.T) {
		if err := store.PutKVPairs([]*dtstruct.KVPair{
			{Key: "fail1", Value: "test"},
			{Key: "fail2", Value: "test2"},
		}); err == nil || err.Error() != "test error" {
			t.Fatalf("Expected %q error from .PutKVPairs(), got: %q", "test error", err.Error())
		}
	})
}

func TestConsulTxnStoreUpdateDatacenterKVPairs(t *testing.T) {
	server := buildConsulTestServer(t, []consulTestServerOp{
		{
			Method: "PUT",
			URL:    "/v1/txn?dc=dc1",
			Request: consulapi.TxnOps{
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVGet, Key: "test"},
				},
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVGet, Key: "test2"},
				},
			},
			Response: &consulapi.TxnResponse{
				Results: consulapi.TxnResults{
					{
						KV: &consulapi.KVPair{Key: "test", Value: []byte("test")},
					},
					{
						KV: &consulapi.KVPair{Key: "test2", Value: []byte("not-equal")},
					},
				},
			},
		},
		{
			Method: "PUT",
			URL:    "/v1/txn?dc=dc1",
			Request: consulapi.TxnOps{
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVGet, Key: "test"},
				},
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVGet, Key: "doesnt-exist"},
				},
			},
			Response: &consulapi.TxnResponse{
				Errors: consulapi.TxnErrors{
					{
						OpIndex: 1,
						What:    `key "doesnt-exist" doesn't exist`,
					},
				},
			},
			ResponseCode: http.StatusConflict, // PUT /v1/txn returns a HTTP 409 code on txn failure
		},
		{
			Method: "PUT",
			URL:    "/v1/txn?dc=dc1",
			Request: consulapi.TxnOps{
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVSet, Key: "test2", Value: []byte("test")},
				},
			},
			Response: &consulapi.TxnResponse{
				Results: consulapi.TxnResults{
					{
						KV: &consulapi.KVPair{Key: "test2", Value: []byte("test")},
					},
				},
			},
		},
		{
			Method: "PUT",
			URL:    "/v1/txn?dc=dc1",
			Request: consulapi.TxnOps{
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVSet, Key: "test", Value: []byte("test")},
				},
				{
					KV: &consulapi.KVTxnOp{Verb: consulapi.KVSet, Key: "doesnt-exist", Value: []byte("test")},
				},
			},
			Response: &consulapi.TxnResponse{
				Results: consulapi.TxnResults{
					{
						KV: &consulapi.KVPair{Key: "test", Value: []byte("test")},
					},
					{
						KV: &consulapi.KVPair{Key: "doesnt-exist", Value: []byte("test")},
					},
				},
			},
		},
	})
	defer server.Close()

	var wg sync.WaitGroup
	config.Config.ConsulAddress = server.URL
	store := NewConsulTxnStore().(*consulTxnStore)

	t.Run("success-cached", func(t *testing.T) {
		wg.Add(1)
		cacheKey := GetConsulKVCacheKey(consulTestDefaultDatacenter, "cached")
		store.kvCache.SetDefault(cacheKey, "cached") // pre-cache the 'cached' key-value
		defer store.kvCache.Flush()

		kvPairs := []*consulapi.KVPair{
			{Key: "cached", Value: []byte("cached")}, // already cached key-value
			{Key: "test", Value: []byte("test")},     // already correct on consul server
			{Key: "test2", Value: []byte("test")},    // not equal on consul server
		}
		skipped, existing, written, failed, err := store.updateDatacenterKVPairs(&wg, consulTestDefaultDatacenter, kvPairs)
		if err != nil {
			t.Fatalf(".updateDatacenterKVPairs() should not return an error, got: %v", err)
		}
		if skipped != 1 || existing != 1 || written != 1 || failed != 0 {
			t.Fatalf("expected: existing/skipped/written=1 and failed=0, got: skipped=%d, existing=%d, written=%d, failed=%d",
				skipped, existing, written, failed,
			)
		}

		for _, pair := range kvPairs {
			cacheKey := GetConsulKVCacheKey(consulTestDefaultDatacenter, pair.Key)
			if cached, found := store.kvCache.Get(cacheKey); !found || cached != string(pair.Value) {
				t.Fatalf("expected cache key %q to equal %q, got %v", cacheKey, string(pair.Value), cached)
			}
		}
	})

	t.Run("success-missing-kv", func(t *testing.T) {
		wg.Add(1)
		kvPairs := []*consulapi.KVPair{
			{Key: "test", Value: []byte("test")},         // already correct on consul server
			{Key: "doesnt-exist", Value: []byte("test")}, // does not exist on consul server
		}
		skipped, existing, written, failed, err := store.updateDatacenterKVPairs(&wg, consulTestDefaultDatacenter, kvPairs)
		if err != nil {
			t.Fatalf(".updateDatacenterKVPairs() should not return an error, got: %v", err)
		}
		if skipped != 0 || existing != 0 || written != 2 || failed != 0 { // confirm all KVs are updated if one does not exist
			t.Fatalf("expected: existing/skipped/failed=0 and written=2, got: skipped=%d, existing=%d, written=%d, failed=%d",
				skipped, existing, written, failed,
			)
		}
	})
}
