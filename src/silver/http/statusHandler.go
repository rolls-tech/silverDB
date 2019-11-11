package http

import (
	"encoding/json"
	"log"
	"net/http"
)

type statusHandler struct {
	*Server
}

func (h *statusHandler) ServeHTTP(w http.ResponseWriter,r *http.Request){
	m:=r.Method
	if m==http.MethodPut{
       w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	b,e:=json.Marshal(h.GetStat())
	if e !=nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

func (s *Server) statusHandler() http.Handler {
	return &statusHandler{s}
}









