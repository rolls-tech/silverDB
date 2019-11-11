package http

import (
	"log"
	"net/http"
	"silver/cache"
)

type Server struct {
	cache.Cache
}

func (s *Server) Listen(){
	http.Handle("/cache",s.cacheHandler())
	http.Handle("/status",s.statusHandler())
	err:=http.ListenAndServe(":12345",nil)
	if err!=nil{
	  log.Println(err.Error())
	}
}

func New(c cache.Cache) *Server{
	return &Server{c}
}
