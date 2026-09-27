package config


import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	yaml "go.yaml.in/yaml/v3"
)


type Source struct {
	Name   string
	Lookup func(string) (string, bool)
}

func Layered(sources ...Source) func(string) (string, bool) {
	return func(name string) (string, bool) {
		base := strings.TrimSuffix(name, "_FILE")
		for _, s := range sources {
			if set(s, base) || set(s, base+"_FILE") {
				return s.Lookup(name)
			}
		}
		return "", false
	}
}

func set(s Source, name string) bool {
	v, ok := s.Lookup(name)
	return ok && v != ""
}


func Sources(lookup func(string) (string, bool)) (func(string) (string, bool), error) {
	env := Source{Name: "environment", Lookup: lookup}
	path, _ := lookup("CONFIG_FILE")
	if path == "" {
		return Layered(env), nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Layered(env), fmt.Errorf("CONFIG_FILE: cannot read %q: %w", path, errors.Unwrap(err))
	}
	file, err := FromYAML(filepath.Base(path), b)
	if err != nil {
		return Layered(env), err
	}
	return Layered(env, file), nil
}

func FromYAML(name string, data []byte) (Source, error) {
	values := map[string]located{}
	src := Source{Name: name, Lookup: func(k string) (string, bool) {
		v, ok := values[k]
		return v.value, ok
	}}

	var errs []error
	fail := func(line int, format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s:%d: %s", name, line, fmt.Sprintf(format, args...)))
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	var doc yaml.Node
	switch err := dec.Decode(&doc); {
	case errors.Is(err, io.EOF): // an empty file is a valid file that sets nothing
		return src, nil
	case err != nil:
		return src, fmt.Errorf("%s: %s", name, strings.TrimPrefix(err.Error(), "yaml: "))
	}
	if dec.Decode(new(yaml.Node)) == nil {
		return src, fmt.Errorf("%s: contains more than one YAML document", name)
	}

	root := &doc
	if root.Kind == yaml.DocumentNode && len(root.Content) == 1 {
		root = root.Content[0]
	}
	switch {
	case root.Kind == yaml.ScalarNode && root.Tag == nullTag:
		return src, nil // a file of nothing but comments
	case root.Kind != yaml.MappingNode:
		return src, fmt.Errorf("%s:%d: the top level must be a mapping of settings", name, root.Line)
	}
	flatten("", "", root, values, fail)
	return src, errors.Join(errs...)
}


type located struct {
	value string
	key   string // dotted path as written in the file
}

const nullTag = "!!null"


var settingName = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

func flatten(prefix, path string, n *yaml.Node, out map[string]located, fail func(int, string, ...any)) {
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], resolve(n.Content[i+1])
		if k.Kind != yaml.ScalarNode {
			fail(k.Line, "keys must be plain names")
			continue
		}
		key, name := k.Value, strings.ToUpper(strings.ReplaceAll(k.Value, "-", "_"))
		if path != "" {
			key = path + "." + key
		}
		if prefix != "" {
			name = prefix + "_" + name
		}
		if !settingName.MatchString(name) {
			fail(k.Line, "%q is not a setting name", key)
			continue
		}
		if v.Kind == yaml.MappingNode {
			flatten(name, key, v, out, fail)
			continue
		}
		value, ok := scalar(key, v, fail)
		if !ok {
			continue
		}
		switch prev, dup := out[name]; {
		case name == "CONFIG_FILE":
			fail(k.Line, "%q: the configuration file cannot name itself; CONFIG_FILE is read from the environment", key)
		case !known()[name]:
			fail(k.Line, "unknown setting %q (%s); see docs/CONFIG.md", key, name)
		case dup:
			fail(k.Line, "%q and %q both set %s", prev.key, key, name)
		default:
			out[name] = located{value: value, key: key}
		}
	}
}

// resolve follows an anchor reference to the node it points at.
func resolve(n *yaml.Node) *yaml.Node {
	for n.Kind == yaml.AliasNode && n.Alias != nil {
		n = n.Alias
	}
	return n
}

// scalar renders a value as the text an environment variable would have carried. A sequence becomes
// the comma-separated list the *list* parsers expect.
func scalar(key string, n *yaml.Node, fail func(int, string, ...any)) (string, bool) {
	switch n.Kind {
	case yaml.ScalarNode:
		if n.Tag == nullTag {
			return "", true // written but empty: the same as unset
		}
		return n.Value, true
	case yaml.SequenceNode:
		items := make([]string, 0, len(n.Content))
		for i, item := range n.Content {
			item = resolve(item)
			switch {
			case item.Kind != yaml.ScalarNode || item.Tag == nullTag:
				fail(item.Line, "%s: item %d must be a plain value", key, i+1)
				return "", false
			case strings.Contains(item.Value, ","):
				fail(item.Line, "%s: item %d contains a comma; write the list as one comma-separated string instead", key, i+1)
				return "", false
			}
			items = append(items, item.Value)
		}
		return strings.Join(items, ","), true
	}
	fail(n.Line, "%s: must be a value, a list or a group of settings", key)
	return "", false
}

// known is the set of names a file may set: every variable, plus the <NAME>_FILE form of each
// secret. It is derived from the one table in config.go, so a new variable needs no change here.
var known = sync.OnceValue(func() map[string]bool {
	var c Config
	m := map[string]bool{}
	for _, f := range c.fields() {
		m[f.name] = true
		if _, secret := f.ptr.(*Secret); secret {
			m[f.name+"_FILE"] = true
		}
	}
	return m
})
