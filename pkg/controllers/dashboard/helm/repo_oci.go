package helm

import (
	"context"
	"time"

	catalog "github.com/rancher/rancher/pkg/apis/catalog.cattle.io/v1"
	"github.com/rancher/rancher/pkg/catalogv2"
	"github.com/rancher/rancher/pkg/catalogv2/oci"
	catalogcontrollers "github.com/rancher/rancher/pkg/generated/controllers/catalog.cattle.io/v1"
	"github.com/rancher/wrangler/v2/pkg/apply"
	"github.com/rancher/wrangler/v2/pkg/condition"
	corev1controllers "github.com/rancher/wrangler/v2/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	"helm.sh/helm/v3/pkg/registry"
	"helm.sh/helm/v3/pkg/repo"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// Ignore non OCI ClusterRepos
	if !registry.IsOCI(clusterRepo.Spec.URL) {
		return clusterRepo, nil
	}

	logrus.Debugf("OCIRepoHandler triggered for clusterrepo %s", clusterRepo.Name)
	var index *repo.IndexFile

	err := ensureIndexConfigMap(clusterRepo, &clusterRepo.Status, o.configMapController)
	if err != nil {
		return o.changeCondition(clusterRepo, err)
	}

	if !shouldRefresh(&clusterRepo.Spec, &clusterRepo.Status) {
		o.clusterRepoController.EnqueueAfter(clusterRepo.Name, interval)
		return clusterRepo, nil
	}

	clusterRepo.Status.ObservedGeneration = clusterRepo.Generation

	secret, err := catalogv2.GetSecret(o.secretCacheController, &clusterRepo.Spec, clusterRepo.Namespace)
	if err != nil {
		return o.changeCondition(clusterRepo, err)
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
		return o.changeCondition(clusterRepo, err)
	}
	index, err = oci.GenerateIndex(clusterRepo.Spec.URL, secret, clusterRepo.Spec, clusterRepo.Status, index)
	if err != nil || index == nil {
		return o.changeCondition(clusterRepo, err)
	}

	clusterRepo.Status.URL = clusterRepo.Spec.URL
	clusterRepo.Status.Branch = ""

	index.SortEntries()

	cm, err := createOrUpdateMap(clusterRepo.Namespace, index, owner, o.apply)
	if err != nil {
		return o.changeCondition(clusterRepo, err)
	}

	clusterRepo.Status.IndexConfigMapName = cm.Name
	clusterRepo.Status.IndexConfigMapNamespace = cm.Namespace
	clusterRepo.Status.IndexConfigMapResourceVersion = cm.ResourceVersion
	clusterRepo.Status.DownloadTime = downloadTime

	return o.changeCondition(clusterRepo, nil)
}

// changeCondition is only called when error happens
func (o *OCIRepohandler) changeCondition(clusterRepo *catalog.ClusterRepo, err error) (*catalog.ClusterRepo, error) {
	repoDownloaded := condition.Cond(catalog.RepoDownloaded)
	if errors.IsConflict(err) {
		repoDownloaded.SetError(&clusterRepo.Status, "", nil)
	} else {
		repoDownloaded.SetError(&clusterRepo.Status, "", err)
	}

	repoDownloaded.LastUpdated(&clusterRepo.Status, time.Now().UTC().Format(time.RFC3339))

	return o.clusterRepoController.UpdateStatus(clusterRepo)
}
