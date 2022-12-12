package server

import (
	"database/sql"
	"errors"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"strconv"

	_ "github.com/lib/pq"
	"golang.org/x/exp/slog"

	"github.com/kakao/varlog/internal/benchmark/model/macrobenchmark"
)

// basepath is the root directory of this package.
var basepath string
var tpl *template.Template

func init() {
	_, currentFile, _, _ := runtime.Caller(0)
	basepath = filepath.Dir(currentFile)

	tpl = template.Must(template.ParseFiles(
		filepath.Join(basepath, "/public/index.tmpl"),
		filepath.Join(basepath, "/public/chart.tmpl"),
	))
}

type Server struct {
	config
	srv http.Server
	db  *sql.DB
}

func New(opts ...Option) (*Server, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.databaseHost, cfg.databasePort, cfg.databaseUser, cfg.databasePassword, cfg.databaseName)
	db, err := sql.Open(databaseDriver, connStr)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}

	s := &Server{
		config: cfg,
		db:     db,
	}

	mux := http.NewServeMux()
	mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir(filepath.Join(basepath, "public", "assets")))))
	mux.HandleFunc("/", s.handler)
	s.srv.Handler = mux
	return s, nil
}

func (s *Server) Run() error {
	const network = "tcp"

	lis, err := net.Listen(network, s.addr)
	if err != nil {
		return err
	}

	url := "http://"
	addr := lis.Addr().(*net.TCPAddr)
	if addr.IP.IsUnspecified() {
		url += "localhost"
	}
	url += ":" + strconv.Itoa(addr.Port)

	slog.Info("starting benchmark server",
		slog.String("url", url),
	)

	err = s.srv.Serve(lis)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Close() error {
	_ = s.srv.Close()
	return s.db.Close()
}

type Workload struct {
	Name          string
	MetricResults []MetricResult
}

type MetricResult struct {
	ChartID   string
	ChartName string
	XValues   []string
	XTitle    string
	YValues   []float64
	YTitle    string
}

func (s *Server) handler(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path != "/" {
		http.NotFound(w, req)
		return
	}

	const limit = 20

	var workloads []Workload
	for _, workloadName := range []string{"one_logstream", "all_logstream"} {
		wk := Workload{
			Name: workloadName,
		}
		for _, metricName := range []string{"append_bytes_per_second", "subscribe_bytes_per_second"} {
			results, err := macrobenchmark.ListMacrobenchmarkResults(s.db, workloadName, metricName, limit)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			chartData := MetricResult{
				ChartID:   workloadName + "-" + metricName,
				ChartName: metricName,
				XTitle:    "commit hash",
				YTitle:    metricName,
			}
			for _, res := range results {
				chartData.XValues = append(chartData.XValues, res.CommitHash)
				chartData.YValues = append(chartData.YValues, res.Values)
			}
			wk.MetricResults = append(wk.MetricResults, chartData)
		}
		workloads = append(workloads, wk)
	}

	if err := tpl.ExecuteTemplate(w, "index", workloads); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
