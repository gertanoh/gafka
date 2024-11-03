package helpers

import "hash/fnv"

func ComputeHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
