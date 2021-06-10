package service

import (
	"fmt"
	"github.com/yametech/devops/pkg/core"
	"github.com/yametech/devops/pkg/store"
	"strconv"
)

type IService interface {
	List(namespace, resource, labels string, sort map[string]interface{}, skip, limit int64) ([]interface{}, error)
	ListByFilter(namespace, resource string, filter, sort map[string]interface{}, skip, limit int64) ([]interface{}, error)
	GetByUUID(namespace, resource, uuid string, result interface{}) error
	GetByFilter(namespace, resource string, result interface{}, filter map[string]interface{}) error
	Create(namespace, resource string, object core.IObject) (core.IObject, error)
	Apply(namespace, resource, uuid string, object core.IObject, forceApply bool) (core.IObject, bool, error)
	Delete(namespace, resource, uuid string) error
	Count(namespace, resource string, filter map[string]interface{}) (int64, error)
	// 上层使用watch，在baseService中调用Watch2
	Watch(namespace, resource, kind, version string, objectChan chan core.IObject, closed chan struct{})
	Watch2(namespace, resource string, resourceVersion int64, watch store.WatchInterface)
}

type BaseService struct {
	store.IKVStore
}

var _ IService = (*BaseService)(nil)

func (bs *BaseService) Watch(namespace, resource, kind, version string, objectChan chan core.IObject, closed chan struct{}) {
	go func(versionStr string) {
		version, err := strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			return
		}
		coder := store.GetResourceCoder(kind)
		if coder == nil {
			return
		}
		wc := store.NewWatch(coder)
		bs.Watch2(namespace, resource, version, wc)
		for {
			select {
			case <-closed:
				wc.CloseStop() <- struct{}{}
				return
			case err := <-wc.ErrorStop():
				fmt.Printf("user watch version: (%d) get error: (%s)\n", version, err)
				close(objectChan)
				return
			case item, ok := <-wc.ResultChan():
				if !ok {
					return
				}
				objectChan <- item
			}
		}
	}(version)
}

func NewBaseService(s store.IKVStore) IService {
	return &BaseService{s}
}
