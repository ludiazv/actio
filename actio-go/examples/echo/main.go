// echo: the agent the conformance suite drives, and the smallest useful program this library
// supports. It answers echo, sleeps, fails, and refuses: the five behaviors a black-box test needs
// to check a peer's acceptance, cancellation, error and reply paths.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "actio echo: this example is not implemented yet")
	os.Exit(1)
}
