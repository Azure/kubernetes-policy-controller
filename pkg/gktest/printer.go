package gktest

// Printer knows how to print the results of running a Suite.
type Printer interface {
	// Print formats and writes SuiteResult to w. If verbose it true, prints more
	// extensive output (behavior is specific to the printer). Returns an error
	// if there is a problem writing to w.
	Print(w StringWriter, r []SuiteResult, verbose bool) error
}

// StringWriter knows how to write a string to a Writer.
//
// Note: StringBuffer meets this interface.
type StringWriter interface {
	WriteString(s string) (int, error)
}
