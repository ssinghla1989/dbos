package api

import "github.com/gin-gonic/gin"

func NewRouter(handlers *Handlers) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/healthz", handlers.Health)

	v1 := router.Group("/v1")
	v1.POST("/schedule-callback", handlers.ScheduleCallback)

	return router
}
