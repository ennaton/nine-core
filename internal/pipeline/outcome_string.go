package pipeline

// String names the outcome in logs and errors. The contract in outcome.go is
// frozen; this file adds a name to it and changes nothing about it. Without
// it every log line in the one package whose subject is classification
// printed a bare integer.
func (o Outcome) String() string {
	switch o {
	case Unknown:
		return "Unknown"
	case Done:
		return "Done"
	case Retry:
		return "Retry"
	case Poison:
		return "Poison"
	case Fatal:
		return "Fatal"
	}
	return "Outcome(" + itoa(int(o)) + ")"
}

// itoa keeps this file free of imports, like the file it names.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
