package gc

type GarbageCollector struct {
	keep       map[string]struct{}
	removeFunc func(string) error
}

func New(removeFunc func(string) error) *GarbageCollector {
	return &GarbageCollector{
		keep:       map[string]struct{}{},
		removeFunc: removeFunc,
	}
}

func (gc *GarbageCollector) Keep(key string) {
	gc.keep[key] = struct{}{}
}

func (gc *GarbageCollector) Reset() {
	gc.keep = map[string]struct{}{}
}

func (gc *GarbageCollector) Collect(key string) error {
	if _, ok := gc.keep[key]; !ok {
		return gc.removeFunc(key)
	}
	return nil
}
