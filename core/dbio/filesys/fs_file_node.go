package filesys

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/gobwas/glob"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// FileNode represents a file node
type FileNode struct {
	URI      string      `json:"uri"`
	IsDir    bool        `json:"is_dir"`
	Size     uint64      `json:"size,omitempty"`
	Created  int64       `json:"created,omitempty"`
	Updated  int64       `json:"updated,omitempty"`
	Owner    string      `json:"owner,omitempty"`
	Columns  iop.Columns `json:"columns,omitempty"`
	Children FileNodes   `json:"children,omitempty"`

	typ  dbio.Type // cached type
	path string    // cached path
}

func (fn *FileNode) MarshalJSON() ([]byte, error) {
	type Alias FileNode
	return json.Marshal(&struct {
		*Alias
		Path string `json:"path"`
		Name string `json:"name"`
	}{
		Alias: (*Alias)(fn),
		Path:  fn.Path(),
		Name:  fn.Name(),
	})
}

func (fn *FileNode) Name() string {
	parts := strings.Split(strings.TrimSuffix(fn.Path(), "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

// Type return the file type
func (fn *FileNode) Type() dbio.Type {

	if fn.typ.String() != "" {
		return fn.typ
	}

	fType, _, _, err := ParseURLType(fn.URI)
	if g.LogError(err) {
		return ""
	}

	fn.typ = fType

	return fn.typ
}

// Path returns the path without the prefix
func (fn *FileNode) Path() string {

	if fn.path != "" {
		return fn.path
	}

	fType, _, path, err := ParseURLType(fn.URI)
	if g.LogError(err) {
		return ""
	}

	if fType == dbio.TypeFileAzure {
		pathContainer := strings.Split(path, "/")[0]

		// remove container
		path = strings.TrimPrefix(path, pathContainer+"/")
	}

	fn.path = path
	fn.typ = fType

	return fn.path
}

// FileNodes represent file nodes
type FileNodes []FileNode

// NewFileNodes creates fileNodes from slice of maps
func NewFileNodes(nodes []map[string]any) (fns FileNodes) {
	fns = make(FileNodes, len(nodes))
	for i, node := range nodes {
		g.Unmarshal(g.Marshal(node), &fns[i])
	}
	return
}

// AddWhere adds a new node to list if pattern matches name and after timestamp
func (fns *FileNodes) AddWhere(pattern *glob.Glob, after int64, ns ...FileNode) {
	nodes := *fns
	for i, n := range ns {
		if strings.HasSuffix(n.URI, "/") {
			ns[i].IsDir = true
		} else if ns[i].IsDir && !strings.HasSuffix(n.URI, "/") {
			ns[i].URI = n.URI + "/"
		}
		if pattern == nil || (*pattern).Match(strings.TrimSuffix(n.Path(), "/")) {
			if after == 0 || ns[i].Updated == 0 || ns[i].Updated > after {
				nodes = append(nodes, ns[i])
			}
		}
	}
	*fns = nodes
}

// Add adds a new node to list
func (fns *FileNodes) Add(ns ...FileNode) {
	nodes := *fns
	for i, n := range ns {
		if strings.HasSuffix(n.URI, "/") {
			ns[i].IsDir = true
		} else if ns[i].IsDir && !strings.HasSuffix(n.URI, "/") {
			ns[i].URI = n.URI + "/"
		}
	}
	nodes = append(nodes, ns...)
	*fns = nodes
}

// URIs give a list of recursive uris
func (fns FileNodes) URIs() (uris []string) {
	for _, p := range fns {
		uris = append(uris, p.URI)
		if p.IsDir {
			uris = append(uris, p.Children.URIs()...)
		}
	}
	return uris
}

// Paths give a list of recursive paths
func (fns FileNodes) Paths() (paths []string) {
	for _, p := range fns {
		paths = append(paths, p.Path())
		if p.IsDir {
			paths = append(paths, p.Children.Paths()...)
		}
	}
	return paths
}

func (fns FileNodes) TotalSize() uint64 {
	total := uint64(0)
	for _, fn := range fns {
		total = total + fn.Size
	}
	return total
}

func (fns FileNodes) AvgSize() uint64 {
	return fns.TotalSize() / uint64(len(fns))
}

// Sort sorts the nodes
func (fns FileNodes) Sort() {
	sort.Slice(fns, func(i, j int) bool {
		val := func(n FileNode) string {
			return cast.ToString(!n.IsDir) + n.URI
		}
		return val(fns[i]) < val(fns[j])
	})
}

// After returns the nodes modified after provided time
func (fns FileNodes) After(ts time.Time) (nodes FileNodes) {
	for _, fn := range fns {
		lastModified := time.Unix(fn.Updated, 0)
		if ts.IsZero() || lastModified.IsZero() || lastModified.After(ts) {
			nodes = append(nodes, fn)
		}
	}
	return
}

// Files returns only files (no folders)
func (fns FileNodes) Files() (nodes FileNodes) {
	for _, fn := range fns {
		if !fn.IsDir {
			nodes = append(nodes, fn)
		}
	}
	return
}

// Folders returns only folders (no files)
func (fns FileNodes) Folders() (nodes FileNodes) {
	for _, fn := range fns {
		if fn.IsDir {
			nodes = append(nodes, fn)
		}
	}
	return
}

// InferFormat returns the most common file format
func (fns FileNodes) InferFormat() (format dbio.FileType) {
	typeCntMap := map[dbio.FileType]int{
		dbio.FileTypeCsv:     0,
		dbio.FileTypeJson:    0,
		dbio.FileTypeParquet: 0,
	}

	for _, fileType := range lo.Keys(typeCntMap) {
		ext := fileType.Ext()
		for _, node := range fns {
			if node.IsDir {
				continue
			}
			path := node.Path()
			if strings.HasSuffix(path, ext) || strings.Contains(path, ext+".") {
				typeCntMap[fileType]++
			}
		}
	}

	max := 0
	for fileType, cnt := range typeCntMap {
		if cnt > max {
			max = cnt
			format = fileType
		}
	}

	return
}

// Columns returns a column map
func (fns FileNodes) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, node := range fns {
		for _, column := range node.Columns {
			key := g.F("%s|%s", node.URI, strings.ToLower(column.Name))
			column.FileURI = node.URI
			columns[key] = column
		}
	}
	return columns
}

// ParseURL parses a URL
func ParseURLType(uri string) (uType dbio.Type, host string, path string, err error) {
	if strings.HasPrefix(uri, "file://") {
		return dbio.TypeFileLocal, "", strings.TrimPrefix(uri, "file://"), nil
	}

	u, err := net.NewURL(uri)
	if err != nil {
		return
	}

	scheme := u.U.Scheme
	host = u.Hostname()

	path = strings.TrimPrefix(uri, g.F("%s://%s/", scheme, u.U.Host))
	// g.Info("uri => %s, host => %s, path => %s (%s)", uri, host, path, u.U.Path)

	if scheme == "" || host == "" {
		err = errors.New("Invalid URL: " + uri)
	}

	// handle azure blob
	if scheme == "https" && strings.HasSuffix(host, ".core.windows.net") {
		return dbio.TypeFileAzure, host, path, nil
	} else if scheme == "https" && strings.Contains(uri, "docs.google.com/spreadsheets") {
		return dbio.TypeFileHTTP, host, path, nil
	} else if g.In(scheme, "http", "https") {
		return dbio.TypeFileHTTP, host, path, nil
	}

	uType, ok := dbio.ValidateType(scheme)
	if !ok {
		err = errors.New("unrecognized url type: " + scheme)
	}

	return
}
