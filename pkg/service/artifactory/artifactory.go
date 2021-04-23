package artifactory

import (
	"fmt"
	"github.com/pkg/errors"
	apiResource "github.com/yametech/devops/pkg/api/resource/artifactory"
	"github.com/yametech/devops/pkg/common"
	"github.com/yametech/devops/pkg/core"
	arResource "github.com/yametech/devops/pkg/resource/artifactory"
	"github.com/yametech/devops/pkg/service"
	"github.com/yametech/go-flowrun"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type ArtifactService struct {
	service.IService
}

func NewArtifact(i service.IService) *ArtifactService {
	return &ArtifactService{i}
}

func (a *ArtifactService) Watch(version string) (chan core.IObject, chan struct{}) {
	objectChan := make(chan core.IObject, 32)
	closed := make(chan struct{})
	a.IService.Watch(common.DefaultNamespace, common.Artifactory, string(arResource.ArtifactKind), version, objectChan, closed)
	return objectChan, closed
}

func (a *ArtifactService) List(name string, page, pageSize int64) ([]interface{}, int64, error) {
	offset := (page - 1) * pageSize
	filter := map[string]interface{}{}
	if name != "" {
		filter["metadata.name"] = bson.M{"$regex": primitive.Regex{Pattern: ".*" + name + ".*", Options: "i"}}
	}
	sort := map[string]interface{}{
		"metadata.version": -1,
	}

	data, err := a.IService.ListByFilter(common.DefaultNamespace, common.Artifactory, filter, sort, offset, pageSize)
	if err != nil {
		return nil, 0, err
	}
	count, err := a.IService.Count(common.DefaultNamespace, common.Artifactory, filter)
	if err != nil {
		return nil, 0, err
	}
	return data, count, nil

}

func (a *ArtifactService) Create(reqAr *apiResource.RequestArtifact) error {
	ar := &arResource.Artifact{
		Spec: arResource.ArtifactSpec{
			GitUrl:   reqAr.GitUrl,
			AppName:  reqAr.AppName,
			Branch:   reqAr.Branch,
			Tag:      reqAr.Tag,
			Remarks:  reqAr.Remarks,
			Language: reqAr.Language,
			Images:   reqAr.ImagesHub,
		},
	}

	ar.GenerateVersion()
	_, err := a.IService.Create(common.DefaultNamespace, common.Artifactory, ar)
	if err != nil {
		return err
	}
	//TODO:sendCIEcho
	arCIInfo := &arResource.ArtifactCIInfo{}
	_ = arCIInfo
	if err := SendCIEcho(ar.Metadata.UUID, arCIInfo); err != nil {
		fmt.Println(err)
	}
	return nil
}

func (a *ArtifactService) GetByUUID(appname string) (*arResource.Artifact, error) {
	ar := &arResource.Artifact{}
	err := a.IService.GetByUUID(common.DefaultNamespace, common.Artifactory, appname, ar)
	if err != nil {
		return nil, err
	}
	return ar, nil
}

func (a *ArtifactService) Update(appname string, reqAr *apiResource.RequestArtifact) (core.IObject, bool, error) {
	ar := &arResource.Artifact{
		Spec: arResource.ArtifactSpec{
			GitUrl:   reqAr.GitUrl,
			AppName:  reqAr.AppName,
			Branch:   reqAr.Branch,
			Tag:      reqAr.Tag,
			Remarks:  reqAr.Remarks,
			Language: reqAr.Language,
			Images:   reqAr.ImagesHub,
		},
	}
	ar.GenerateVersion()
	return a.IService.Apply(common.DefaultNamespace, common.Artifactory, appname, ar, false)
}

func (a *ArtifactService) Delete(appname string) error {
	err := a.IService.Delete(common.DefaultNamespace, common.Artifactory, appname)
	if err != nil {
		return err
	}
	return nil
}

func SendCIEcho(uuid string, a *arResource.ArtifactCIInfo) error {
	if uuid == "" {
		return errors.New("UUID is not none")
	}

	flowRun := &flowrun.FlowRun{
		EchoerUrl: common.EchoerUrl,
		Name:      fmt.Sprintf("%s_%d", common.DefaultNamespace, time.Now().UnixNano()),
	}
	flowRunStep := map[string]string{
		"SUCCESS": "done", "FAIL": "done",
	}
	flowRunAction, err := core.ToMap(a)
	if err != nil {
		return err
	}

	flowRunStepName := fmt.Sprintf("PRODCI_%s", uuid)
	flowRun.AddStep(flowRunStepName, flowRunStep, common.EchoerCI, flowRunAction)

	flowRunData := flowRun.Generate()
	fmt.Println(flowRunData)
	if !flowRun.Create(flowRunData) {
		return errors.New("send fsm error")
	}
	return nil
}
