package wasm

import (
	"context"
	"fmt"

	"github.com/tetratelabs/wazero/api"

	"github.com/streamingfast/substreams/storage/store"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type Call struct {
	ctx context.Context

	moduleName string
	wasmModule *Module
	entrypoint string

	inputStores  []store.Reader
	outputStore  store.Store
	updatePolicy pbsubstreams.Module_KindStore_UpdatePolicy

	valueType string

	clock *pbsubstreams.Clock // Used by WASM extensions

	returnValue []byte
	panicError  *PanicError

	Logs           []string
	LogsByteCount  uint64
	ExecutionStack []string
}

//func (m *Module) NewCall(clock *pbsubstreams.Clock, moduleName string, entrypoint string, arguments []Argument) (*Call, error) {
// FIXME: that's to prevent calls when context was closed, protect in the caller?
//if m.isClosed {
//	panic("module is closed")
//}

// FIXME: Replace by `context.Context`, and should speed up execution.
//if i.registry.maxFuel != 0 {
//	if remaining, _ := i.wasmStore.ConsumeFuel(i.registry.maxFuel); remaining != 0 {
//		i.wasmStore.ConsumeFuel(remaining) // don't accumulate fuel from previous executions
//	}
//	i.wasmStore.AddFuel(i.registry.maxFuel)
//}
//}

func (m *Module) ExecuteNewCall(ctx context.Context, cachedInstance api.Module, clock *pbsubstreams.Clock, moduleName string, entrypoint string, arguments []Argument) (mod api.Module, call *Call, err error) {
	//if cachedInstance != nil {
	//	mod = cachedInstance
	//} else {
	_, mod, err = m.instantiateModule(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("could not instantiate wasm module for %q: %w", moduleName, err)
	}
	defer mod.Close(ctx) // Otherwise, deferred to the BaseExecutor.Close() when cached.
	//defer runtime.Close(ctx)
	//}

	f := mod.ExportedFunction(entrypoint)
	if f == nil {
		return mod, nil, fmt.Errorf("could not find entrypoint function %q for module %q", entrypoint, moduleName)
	}

	call = &Call{
		moduleName: moduleName,
		clock:      clock,
		wasmModule: m,
		entrypoint: entrypoint,
	}

	var args []uint64
	for _, input := range arguments {
		switch v := input.(type) {
		case *StoreWriterOutput:
			call.outputStore = v.Store
			call.updatePolicy = v.UpdatePolicy
			call.valueType = v.ValueType
		case *StoreReaderInput:
			call.inputStores = append(call.inputStores, v.Store)
			args = append(args, uint64(len(call.inputStores)-1))
		case ValueArgument:
			cnt := v.Value()
			ptr := writeToHeap(ctx, mod, cnt, input.Name())
			length := uint64(len(cnt))
			args = append(args, uint64(ptr), length)
		default:
			panic("unknown wasm argument type")
		}
	}

	_, err = f.Call(withContext(ctx, call), args...)
	if err != nil {
		if call.panicError != nil {
			return mod, call, call.panicError
		}
		return mod, call, fmt.Errorf("executing module %q: %w", call.moduleName, err)
	}

	return mod, call, nil
}

func (c *Call) Err() error {
	return c.panicError
}

func (c *Call) Output() []byte {
	return c.returnValue
}

func (c *Call) SetOutputStore(store store.Store) {
	c.outputStore = store
}

const maxLogByteCount = 128 * 1024 // 128 KiB

func (c *Call) ReachedLogsMaxByteCount() bool {
	return c.LogsByteCount >= maxLogByteCount
}

func (c *Call) validateSetStore(key string) {
	c.validateSimple("set", pbsubstreams.Module_KindStore_UPDATE_POLICY_SET, key)
}
func (c *Call) validateSetIfNotExists(key string) {
	c.validateSimple("set_if_not_exists", pbsubstreams.Module_KindStore_UPDATE_POLICY_SET_IF_NOT_EXISTS, key)
}
func (c *Call) validateAppend(key string) {
	c.validateSimple("append", pbsubstreams.Module_KindStore_UPDATE_POLICY_APPEND, key)
}
func (c *Call) validateAddBigInt(key string) {
	c.validateWithValueType("add_bigint", pbsubstreams.Module_KindStore_UPDATE_POLICY_ADD, "bigint", key)
}
func (c *Call) validateAddBigDecimal(key string) {
	c.validateWithTwoValueTypes("add_bigdecimal", pbsubstreams.Module_KindStore_UPDATE_POLICY_ADD, "bigdecimal", "bigfloat", key)
}
func (c *Call) validateAddInt64(key string) {
	c.validateWithValueType("add_int64", pbsubstreams.Module_KindStore_UPDATE_POLICY_ADD, "int64", key)
}
func (c *Call) validateAddFloat64(key string) {
	c.validateWithValueType("add_float64", pbsubstreams.Module_KindStore_UPDATE_POLICY_ADD, "float64", key)
}
func (c *Call) validateSetMinInt64(key string) {
	c.validateWithValueType("set_min_int64", pbsubstreams.Module_KindStore_UPDATE_POLICY_MIN, "int64", key)
}
func (c *Call) validateSetMinBigInt(key string) {
	c.validateWithValueType("set_min_bigint", pbsubstreams.Module_KindStore_UPDATE_POLICY_MIN, "bigint", key)
}
func (c *Call) validateSetMinFloat64(key string) {
	c.validateWithValueType("set_min_float64", pbsubstreams.Module_KindStore_UPDATE_POLICY_MIN, "float64", key)
}
func (c *Call) validateSetMinBigDecimal(key string) {
	c.validateWithTwoValueTypes("set_min_bigdecimal", pbsubstreams.Module_KindStore_UPDATE_POLICY_MIN, "bigdecimal", "bigfloat", key)
}
func (c *Call) validateSetMaxInt64(key string) {
	c.validateWithValueType("set_max_int64", pbsubstreams.Module_KindStore_UPDATE_POLICY_MAX, "int64", key)
}
func (c *Call) validateSetMaxBigInt(key string) {
	c.validateWithValueType("set_max_bigint", pbsubstreams.Module_KindStore_UPDATE_POLICY_MAX, "bigint", key)
}
func (c *Call) validateSetMaxFloat64(key string) {
	c.validateWithValueType("set_max_float64", pbsubstreams.Module_KindStore_UPDATE_POLICY_MAX, "float64", key)
}
func (c *Call) validateSetMaxBigDecimal(key string) {
	c.validateWithTwoValueTypes("set_max_bigdecimal", pbsubstreams.Module_KindStore_UPDATE_POLICY_MAX, "bigdecimal", "bigfloat", key)
}

func (c *Call) validateSimple(stateFunc string, updatePolicy pbsubstreams.Module_KindStore_UpdatePolicy, key string) {
	if c.updatePolicy != updatePolicy {
		c.returnInvalidPolicy(stateFunc, fmt.Sprintf(`updatePolicy == %q`, policyMap[updatePolicy]))
	}
	c.traceStateWrites(stateFunc, key)
}

func (c *Call) validateWithValueType(stateFunc string, updatePolicy pbsubstreams.Module_KindStore_UpdatePolicy, valueType string, key string) {
	if c.updatePolicy != updatePolicy || c.valueType != valueType {
		c.returnInvalidPolicy(stateFunc, fmt.Sprintf(`updatePolicy == %q and valueType == %q`, policyMap[updatePolicy], valueType))
	}
	c.traceStateWrites(stateFunc, key)
}

func (c *Call) validateWithTwoValueTypes(stateFunc string, updatePolicy pbsubstreams.Module_KindStore_UpdatePolicy, valueType1, valueType2 string, key string) {
	if c.updatePolicy != updatePolicy || (c.valueType != valueType1 && c.valueType != valueType2) {
		c.returnInvalidPolicy(stateFunc, fmt.Sprintf(`updatePolicy == %q and valueType == %q`, policyMap[updatePolicy], valueType1))
	}
	c.traceStateWrites(stateFunc, key)
}

func (c *Call) traceStateWrites(stateFunc, key string) {
	store := c.outputStore
	line := fmt.Sprintf("%s::%s key: %q, store details: %s", store.Name(), stateFunc, key, store.String())
	c.ExecutionStack = append(c.ExecutionStack, line)
}

func (c *Call) traceStateReads(stateFunc string, storeIndex int, found bool, key string) {
	store := c.inputStores[storeIndex]
	line := fmt.Sprintf("%s::%s key: %q, found: %v, store details: %s", store.Name(), stateFunc, key, found, store.String())
	c.ExecutionStack = append(c.ExecutionStack, line)
}

func (c *Call) returnInvalidPolicy(stateFunc, policy string) {
	panic(fmt.Errorf("module %q: invalid store operation %q, only valid for stores with %s", c.moduleName, stateFunc, policy))
}

func (c *Call) returnError(err error) {
	panic(fmt.Errorf("module %q: %w", c.moduleName, err))
}

var policyMap = map[pbsubstreams.Module_KindStore_UpdatePolicy]string{
	pbsubstreams.Module_KindStore_UPDATE_POLICY_UNSET:             "unset",
	pbsubstreams.Module_KindStore_UPDATE_POLICY_SET:               "replace",
	pbsubstreams.Module_KindStore_UPDATE_POLICY_SET_IF_NOT_EXISTS: "ignore",
	pbsubstreams.Module_KindStore_UPDATE_POLICY_ADD:               "add",
	pbsubstreams.Module_KindStore_UPDATE_POLICY_MIN:               "min",
	pbsubstreams.Module_KindStore_UPDATE_POLICY_MAX:               "max",
	pbsubstreams.Module_KindStore_UPDATE_POLICY_APPEND:            "append",
}
