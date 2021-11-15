package martini

import (
	"log"
	"net/http"
	"time"
)

const TimeFormat = "2006-01-02 15:04:05"

// Logger returns a middleware handler that logs the request as it goes in and the response as it goes out.
func Logger() Handler {
	return func(res http.ResponseWriter, req *http.Request, c Context, log *log.Logger) {
		start := time.Now()

		addr := req.Header.Get("X-Real-IP")
		if addr == "" {
			addr = req.Header.Get("X-Forwarded-For")
			if addr == "" {
				addr = req.RemoteAddr
			}
		}

		log.Printf("%s %s [martini] Started %s %s for %s", time.Now().Format(TimeFormat), "INFO", req.Method, req.URL.Path, addr)

		rw := res.(ResponseWriter)
		c.Next()

		log.Printf("%s %s [martini] Completed %v %s in %v\n", time.Now().Format(TimeFormat), "INFO", rw.Status(), http.StatusText(rw.Status()), time.Since(start))
	}
}
