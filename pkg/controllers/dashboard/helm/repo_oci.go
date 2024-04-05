package helm

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	catalog "github.com/rancher/rancher/pkg/apis/catalog.cattle.io/v1"
	"github.com/rancher/rancher/pkg/catalogv2"
	"github.com/rancher/rancher/pkg/catalogv2/oci"
	catalogcontrollers "github.com/rancher/rancher/pkg/generated/controllers/catalog.cattle.io/v1"
	corev1 "github.com/rancher/rancher/pkg/generated/norman/core/v1"
	"github.com/rancher/wrangler/v2/pkg/apply"
	"github.com/rancher/wrangler/v2/pkg/condition"
	corev1controllers "github.com/rancher/wrangler/v2/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	"helm.sh/helm/v3/pkg/registry"
	"helm.sh/helm/v3/pkg/repo"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"oras.land/oras-go/v2/registry/remote/errcode"
)

type OCIRepohandler struct {
	clusterRepoController catalogcontrollers.ClusterRepoController
	configMapController   corev1controllers.ConfigMapController
	secretCacheController corev1controllers.SecretCache
	apply                 apply.Apply
}

func RegisterOCIRepo(ctx context.Context,
	apply apply.Apply,
	clusterRepoController catalogcontrollers.ClusterRepoController,
	configMapController corev1controllers.ConfigMapController,
	secretsController corev1controllers.SecretCache) {

	ociRepoHandler := &OCIRepohandler{
		clusterRepoController: clusterRepoController,
		configMapController:   configMapController,
		secretCacheController: secretsController,
		apply:                 apply.WithCacheTypes(configMapController).WithStrictCaching().WithSetOwnerReference(false, false),
	}

	clusterRepoController.OnChange(ctx, "oci-clusterrepo-helm", ociRepoHandler.onClusterRepoChange)
}

// Retrigger every 6 hours
func (o *OCIRepohandler) onClusterRepoChange(key string, clusterRepo *catalog.ClusterRepo) (*catalog.ClusterRepo, error) {
	if clusterRepo == nil {
		return nil, nil
	}

	orignalStatus := clusterRepo.Status.DeepCopy()

	// Ignore non OCI ClusterRepos
	if !registry.IsOCI(clusterRepo.Spec.URL) {
		return clusterRepo, nil
	}

	logrus.Debugf("OCIRepoHandler triggered for clusterrepo %s", clusterRepo.Name)
	var index *repo.IndexFile

	err := ensureIndexConfigMap(clusterRepo, &clusterRepo.Status, o.configMapController)
	if err != nil {
		return o.setErrorCondition(clusterRepo, err, orignalStatus)
	}

	if !shouldRefresh(&clusterRepo.Spec, &clusterRepo.Status) {
		o.clusterRepoController.EnqueueAfter(clusterRepo.Name, interval)
		return clusterRepo, nil
	}

	clusterRepo.Status.ObservedGeneration = clusterRepo.Generation

	secret, err := catalogv2.GetSecret(o.secretCacheController, &clusterRepo.Spec, clusterRepo.Namespace)
	if err != nil {
		return o.setErrorCondition(clusterRepo, err, orignalStatus)
	}

	owner := metav1.OwnerReference{
		APIVersion: catalog.SchemeGroupVersion.Group + "/" + catalog.SchemeGroupVersion.Version,
		Kind:       "ClusterRepo",
		Name:       clusterRepo.Name,
		UID:        clusterRepo.UID,
	}

	downloadTime := metav1.Now()
	index, err = getIndexfile(clusterRepo.Status, clusterRepo.Spec, o.configMapController, owner, clusterRepo.Namespace)
	if err != nil {
		return o.setErrorCondition(clusterRepo, err, orignalStatus)
	}
	index, err = oci.GenerateIndex(clusterRepo.Spec.URL, secret, clusterRepo.Spec, clusterRepo.Status, index)
	// If there is 401 or 403 error code, then we don't reconcile further and wait for 6 hours interval
	var errResp *errcode.ErrorResponse
	if errors.As(err, &errResp) {
		if errResp.StatusCode == http.StatusUnauthorized ||
			errResp.StatusCode == http.StatusForbidden ||
			errResp.StatusCode == http.StatusNotFound {
			return o.set4xxCondition(clusterRepo, errResp, orignalStatus)
		}

		if errResp.StatusCode == http.StatusTooManyRequests {
			clusterRepo.Status.URL = clusterRepo.Spec.URL
			clusterRepo.Status.Branch = ""

			index.SortEntries()
			_, err := createOrUpdateMap(clusterRepo.Namespace, index, owner, o.apply)
			if err != nil {
				logrus.Debugf("failed to create/udpate the configmap incase of 4xx statuscode for %s", clusterRepo.Name)
				return o.set4xxCondition(clusterRepo, errResp, orignalStatus)
			}

			return o.set4xxCondition(clusterRepo, errResp, orignalStatus)
		}
	}

	// If there is 429 error code, then we don't reconcile further and wait for 6 hours interval
	// but we also create the configmap for future usecases.
	if err != nil || index == nil {
		return o.setErrorCondition(clusterRepo, err, orignalStatus)
	}

	clusterRepo.Status.URL = clusterRepo.Spec.URL
	clusterRepo.Status.Branch = ""

	index.SortEntries()
	cm, err := createOrUpdateMap(clusterRepo.Namespace, index, owner, o.apply)
	if err != nil {
		return o.setErrorCondition(clusterRepo, err, orignalStatus)
	}

	clusterRepo.Status.IndexConfigMapName = cm.Name
	clusterRepo.Status.IndexConfigMapNamespace = cm.Namespace
	clusterRepo.Status.IndexConfigMapResourceVersion = cm.ResourceVersion
	clusterRepo.Status.DownloadTime = downloadTime

	return o.setErrorCondition(clusterRepo, err, orignalStatus)
}

// setErrorCondition is only called when error happens in the handler and
// we need to depend on wrangler to reenqueue the handler
func (o *OCIRepohandler) setErrorCondition(clusterRepo *catalog.ClusterRepo, err error, orignalStatus *catalog.RepoStatus) (*catalog.ClusterRepo, error) {
	var statusErr error

	ociDownloaded := condition.Cond(catalog.OCIDownloaded)
	if apierrors.IsConflict(err) {
		ociDownloaded.SetError(&clusterRepo.Status, "", nil)
	} else {
		ociDownloaded.SetError(&clusterRepo.Status, "", err)
	}

	if !equality.Semantic.DeepEqual(orignalStatus, clusterRepo.Status) {
		ociDownloaded.LastUpdated(&clusterRepo.Status, time.Now().UTC().Format(time.RFC3339))

		clusterRepo, statusErr = o.clusterRepoController.UpdateStatus(clusterRepo)
		if statusErr != nil {
			err = statusErr
		}
	}

	return clusterRepo, err
}

// set4xxCondition is only called when we receive a 4xx error
// we need to wait for 6 hours to reenqueue.
func (o *OCIRepohandler) set4xxCondition(clusterRepo *catalog.ClusterRepo, err *errcode.ErrorResponse, orignalStatus *catalog.RepoStatus) (*catalog.ClusterRepo, error) {
	err.Errors = append(err.Errors, errcode.Error{Message: fmt.Sprintf(" will retry will after %s", interval)})

	ociDownloaded := condition.Cond(catalog.OCIDownloaded)
	if apierrors.IsConflict(err) {
		ociDownloaded.SetError(&clusterRepo.Status, "", nil)
	} else {
		ociDownloaded.SetError(&clusterRepo.Status, "", err)
	}

	if !equality.Semantic.DeepEqual(orignalStatus, &clusterRepo.Status) {
		// Since status has changed, update the lastUpdatedTime
		ociDownloaded.LastUpdated(&clusterRepo.Status, time.Now().UTC().Format(time.RFC3339))

		return o.clusterRepoController.UpdateStatus(clusterRepo)
	}

	return clusterRepo, nil
}

// getIndexfile fetches the indexfile if it already exits for the clusterRepo
// if not, it creates a new indexfile and returns it.
func getIndexfile(clusterRepoStatus catalog.RepoStatus,
	clusterRepoSpec catalog.RepoSpec,
	configMapClient corev1controllers.ConfigMapClient,
	owner metav1.OwnerReference,
	namespace string) (*repo.IndexFile, error) {

	indexFile := repo.NewIndexFile()
	var configMap *corev1.ConfigMap
	var err error

	if clusterRepoSpec.URL != clusterRepoStatus.URL {
		return indexFile, nil
	}

	// If the status has the configmap defined, fetch it.
	if clusterRepoStatus.IndexConfigMapName != "" {
		configMap, err = configMapClient.Get(clusterRepoStatus.IndexConfigMapNamespace, clusterRepoStatus.IndexConfigMapName, metav1.GetOptions{})
		if err != nil {
			return indexFile, fmt.Errorf("failed to fetch the index configmap for clusterRepo %s", owner.Name)
		}
	} else {
		// otherwise if the configmap is already created, fetch it using the name of the configmap and the namespace.
		configMapName := generateConfigMapName(owner.Name, 0, owner.UID)
		configMapNamespace := getConfigMapNamespace(namespace)

		configMap, err = configMapClient.Get(configMapNamespace, configMapName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return indexFile, nil
			}
			return indexFile, fmt.Errorf("failed to fetch the index configmap for clusterRepo %s", owner.Name)
		}
	}

	data, err := readBytes(configMapClient, configMap)
	if err != nil {
		return indexFile, fmt.Errorf("failed to read bytes of existing configmap for URL %s", clusterRepoSpec.URL)
	}
	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return indexFile, err
	}
	defer gz.Close()
	data, err = io.ReadAll(gz)
	if err != nil {
		return indexFile, err
	}
	if err := json.Unmarshal(data, indexFile); err != nil {
		return indexFile, err
	}

	return indexFile, nil
}
