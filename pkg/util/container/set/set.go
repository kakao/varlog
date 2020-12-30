package set

type Set map[interface{}]struct{}

func New(initSize int) Set {
	set := make(Set, initSize)
	return set
}

func (s Set) Add(k interface{}) {
	s[k] = struct{}{}
}

func (s Set) Remove(k interface{}) {
	delete(s, k)
}

func (s Set) Contains(k interface{}) bool {
	_, ok := s[k]
	return ok
}

func (s Set) Size() int {
	return len(s)
}

func (s Set) Foreach(f func(interface{}) bool) {
	for k := range s {
		if !f(k) {
			return
		}
	}
}

func (s Set) Diff(sub Set) Set {
	r := New(s.Size())
	s.Foreach(func(k interface{}) bool {
		if !sub.Contains(k) {
			r.Add(k)
		}

		return true
	})

	return r
}
