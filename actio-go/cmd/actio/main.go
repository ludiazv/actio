// The actio command: ls, diarium, call, cmd, broadcast, validate over one shared actio root. It
// uses only the public API of the library, which is why those four entry points are exported.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "actio: the command is not implemented yet")
	os.Exit(1)
}
