package http

import (

)

/*type Server struct {
	cluster.Storage
	cluster.Node
}

func (s *Server) Listen(addr string){
	http.Handle("/cluster",s.clusterHandler())
	err:=http.ListenAndServe(addr,nil)
	if err!=nil {
		log.Println(err.Error())
	}
}

func New(c cluster.Storage,n cluster.Node) *Server{
	return &Server{c,n}
}*/
