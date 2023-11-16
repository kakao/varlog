package server

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"

	_ "github.com/lib/pq"

	"github.com/kakao/varlog/internal/benchmark/model/macro/metric"
	"github.com/kakao/varlog/internal/benchmark/model/macro/result"
)

//go:embed public/*
var content embed.FS

var tpl = template.Must(template.ParseFS(content, "public/*.tmpl", "public/assets/*"))

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
	mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(content))))
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
	Name   string
	Metric []WorkloadMetric
}

type WorkloadMetric struct {
	MetricName string
	Targets    []WorkloadTarget
}
type WorkloadTarget struct {
	TargetName string

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
	for _, workloadName := range []string{
		"tp1_ls1_msg128_batch10_app5_sub0",
		"tp1_ls4_msg128_batch10_app5_sub0",
		"tp4_ls1_msg128_batch10_app5_sub0",
		"tp4_ls4_msg128_batch10_app5_sub0",
	} {
		wk := Workload{
			Name: workloadName,
		}
		for _, metricName := range []string{
			metric.AppendRequestsPerSecond,
			metric.AppendBytesPerSecond,
			metric.AppendDurationMillis,
		} {
			results, err := result.ListMacrobenchmarkResults(s.db, workloadName, metricName, limit)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			var tgts []string
			wts := make(map[string]WorkloadTarget)
			for _, r := range results {
				targetName := r.Target
				tgts = append(tgts, targetName)
				wt, ok := wts[targetName]
				if !ok {
					wt = WorkloadTarget{
						TargetName: targetName,
						ChartID:    strings.Join([]string{workloadName, metricName, targetName}, "-"),
						ChartName:  metricName + "#" + targetName,
						XTitle:     "commit hash",
						YTitle:     metricName,
					}
				}
				wt.XValues = append(wt.XValues, r.CommitHash)
				wt.YValues = append(wt.YValues, r.Value)
				wts[targetName] = wt
			}
			sort.Strings(tgts)
			wkm := WorkloadMetric{MetricName: metricName}
			for _, targetName := range tgts {
				wkm.Targets = append(wkm.Targets, wts[targetName])
			}
			wk.Metric = append(wk.Metric, wkm)
		}
		workloads = append(workloads, wk)
	}

	if err := tpl.ExecuteTemplate(w, "index", workloads); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
