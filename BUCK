# The Go checks of this repository as Buck2 targets (munhq/desk#244): the rules are the workflow buck-node, mounted by `checkouts`
# of .desk/config.json. `gofmt -l` and `go vet ./...` are cached actions; the check is //:check.
load("@desk_node//:defs.bzl", "go_checks")

go_checks(
    # Every file but the documents: `go:embed` reads files of any type, and a nested go.mod makes a folder another module.
    srcs = glob(["**"], exclude = ["**/*.md", "Dockerfile", "LICENSE", ".desk/**", ".desk-work/**", ".git/**", "buck-out/**", "BUCK", ".buckconfig"]),
)
