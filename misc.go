package main

func min(vals ...int) int {
	var best int
	switch len(vals) {
	case 0:
		return 0
	case 1:
		return vals[0]
	default:
		best = vals[0]
	}
	for _, v := range vals[1:] {
		if v < best {
			best = v
		}
	}
	return best
}
