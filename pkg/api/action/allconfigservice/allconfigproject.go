package allconfigservice

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/yametech/devops/pkg/api"
	apiResource "github.com/yametech/devops/pkg/api/resource"
	"net/http"
)

func (s *Server) ListGlobalConfig(g *gin.Context) {
	search := g.DefaultQuery("search", "")
	uuid := g.DefaultQuery("uuid", "")
	res, err := s.AllConfigService.GetByUUID(search, uuid)
	if err != nil {
		api.RequestParamsError(g, "error", err)
		return
	}
	g.JSON(http.StatusOK, gin.H{"data": res})
}

func (s *Server) CreateGlobalConfig(g *gin.Context) {
	rawData, err := g.GetRawData()
	if err != nil {
		api.RequestParamsError(g, "error", err)
		return
	}
	request := &apiResource.RequestGlobalConfig{}
	if err := json.Unmarshal(rawData, &request); err != nil {
		api.RequestParamsError(g, "unmarshal json error", err)
		return
	}
	err = s.AllConfigService.Create(request)
	if err != nil {
		api.RequestParamsError(g, "creat allConfig error", err)
		return
	}
	g.JSON(http.StatusOK, request)
}

func (s *Server) UpdateGlobalConfig(g *gin.Context) {
	uuid := g.Param("uuid")
	rawData, err := g.GetRawData()
	if err != nil {
		api.RequestParamsError(g, "get rawData error", err)
		return
	}
	request := &apiResource.RequestGlobalConfig{}
	if err := json.Unmarshal(rawData, &request); err != nil {
		api.RequestParamsError(g, "unmarshal json error", err)
		return
	}
	data, _, err := s.AllConfigService.Update(uuid, request)
	if err != nil {
		api.RequestParamsError(g, "update fail", err)
		return
	}
	g.JSON(http.StatusOK, gin.H{"data": data})
}
