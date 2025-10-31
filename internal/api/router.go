package api

import "github.com/gin-gonic/gin"

func NewRouter(handlers *Handlers) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/healthz", handlers.Health)

	return router
}
